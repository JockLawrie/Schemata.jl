module diagnosedata

export diagnose, enforce_schema

using CSV
using DataFrames
using CategoricalArrays
using Tables

using ..CustomParsers
using ..handle_validvalues
using ..types

################################################################################
# API functions

"""
Returns: Vector of NamedTuples, each of which is a way in which the table does not comply with the schema.

Example result:
  :entity   :id        :issue
   col      patientid  Incorrect data type (String)
   col      patientid  Missing data not allowed
   col      patientid  Values are not unique
   col      gender     Invalid values ('d')
   table    mytable    Primary key not unique

There are 2 methods for diagnosing a table:

1. `diagnose(table, tableschema)` diagnoses an in-memory table.

2. `diagnose(datafile::String, tableschema)` diagnoses a table located at `datafile`.
   This method is designed for tables that are too big for RAM.
   It diagnoses a table one row at a time.
"""
diagnose(table, tableschema::TableSchema) = diagnose_inmemory_table(table, tableschema, false)
enforce_schema(table, tableschema)        = diagnose_inmemory_table(table, tableschema, true)

diagnose(datafile::String, tableschema::TableSchema)           = diagnose_streaming_table(datafile, tableschema, false, "")
enforce_schema(datafile::String, tableschema, outfile::String) = diagnose_streaming_table(datafile, tableschema, true,  outfile)

################################################################################
# diagnose_inmemory_table!

function diagnose_inmemory_table(table, tableschema::TableSchema, enforce::Bool)
    # Init
    tablename     = tableschema.name
    issues        = NamedTuple{(:entity, :id, :issue), Tuple{String, String, String}}[]
    outdata       = enforce ? init_outdata(tableschema, size(table, 1)) : nothing
    tableissues   = Dict(:primarykey_isunique => true, :intrarow_constraints => Set{String}())
    columnissues  = Dict(colname => Dict(:n_notunique => 0, :n_missing => 0, :n_invalid => 0) for colname in keys(tableschema.colname2colschema))
    colnames      = propertynames(table)
    primarykey    = fill("", length(tableschema.primarykey))
    pk_colnames   = tableschema.primarykey
    pkvalues      = Set{String}()  # Values of the primary key
    uniquevalues  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in tableschema.colname2colschema if colschema.isunique==true)
    rowdict       = Dict{Symbol, Any}()
    i_data        = 0
    nconstraints  = length(tableschema.intrarow_constraints)
    colname2colschema = tableschema.colname2colschema
    datacols_match_schemacols!(issues, tableschema, Set(colnames))  # Run this on table (not on outdata) so that missing indata columns are reported

    # Row-level checks
    for row in Tables.rows(table)
        i_data += 1
        if length(pk_colnames) > 1 && tableissues[:primarykey_isunique] # Uniqueness of 1-column primary keys is checked at the column level
            j = 0
            for colname in colnames
                j  += 1
                val = getproperty(row, colname)
                primarykey[j] = ismissing(val) ? "" : string(val)
            end
            primarykey_isunique!(tableissues, primarykey, pkvalues)
        end
        if enforce
            for colname in propertynames(row)      # Copy row to rowdict
                rowdict[colname] = getproperty(row, colname)
            end
            parserow!(colname2colschema, rowdict)  # Parse rowdict
            for (colname, val) in rowdict          # Write rowdict to outdata
                ismissing(val) && continue
                !haskey(colname2colschema, colname) && continue
                colschema = colname2colschema[colname]
                outdata[i_data, colname] = value_is_valid(val, colschema.validvalues) ? val : missing
            end
            assess_row!(tableissues, columnissues, rowdict, tableschema, uniquevalues)
            length(tableissues[:intrarow_constraints]) == nconstraints && continue # All constraints have already been breached
            test_intrarow_constraints!(tableissues[:intrarow_constraints], tableschema, rowdict)
        else
            assess_row!(tableissues, columnissues, row, tableschema, uniquevalues)
            length(tableissues[:intrarow_constraints]) == nconstraints && continue # All constraints have already been breached
            for colname in propertynames(row)
                rowdict[colname] = getproperty(row, colname)
            end
            test_intrarow_constraints!(tableissues[:intrarow_constraints], tableschema, rowdict)
        end
    end

    # Column-level checks
    for (colname, colschema) in colname2colschema
        if enforce && colschema.iscategorical
            categorical!(outdata, colname)
        end
        coldata    = enforce ? getproperty(outdata, colname) : getproperty(table, colname)
        data_eltyp = colschema.iscategorical ? eltype(levels(coldata)) : Core.Compiler.typesubtract(eltype(coldata), Missing)
        if data_eltyp != colschema.datatype  # Check data type matches that specified in the ColumnSchema
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="Data has type $(data_eltyp), schema requires $(colschema.datatype)."))
        end
        if colschema.iscategorical && !(coldata isa CategoricalVector)  # Ensure categorical values
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="Data is not categorical."))
        end
    end

    # Format result
    issues = storeissues(issues, tableissues, columnissues, tablename, i_data)
    !enforce && return issues
    outdata, issues
end

################################################################################
# diagnose_streaming_table!

function diagnose_streaming_table(infile::String, tableschema::TableSchema, enforce::Bool, outfile::String)
    if enforce
        outdir    = dirname(outfile)  # outdir = "" means outfile is in the pwd()
        outdir   != "" && !isdir(outdir) && error("The directory containing the specified output file does not exist.")
        outdata   = init_outdata(infile, tableschema)
        n_outdata = size(outdata, 1)
        CSV.write(outfile, init_outdata(tableschema, 0))  # Write column headers to disk
    else
        outdata   = nothing
        n_outdata = 0
    end
    tablename     = tableschema.name
    issues        = NamedTuple{(:entity, :id, :issue), Tuple{String, String, String}}[]
    tableissues   = Dict(:primarykey_isunique => true, :intrarow_constraints => Set{String}())
    columnissues  = Dict(colname => Dict(:n_notunique => 0, :n_missing => 0, :n_invalid => 0) for colname in keys(tableschema.colname2colschema))
    colnames      = nothing
    colnames_done = false
    pk_colnames   = tableschema.primarykey
    primarykey    = fill("", length(pk_colnames))
    pkvalues      = Set{String}()  # Values of the primary key
    uniquevalues  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in tableschema.colname2colschema if colschema.isunique==true)
    delim         = infile[(end - 2):end] == "csv" ? "," : "\t"
    row           = Dict{Symbol, Any}()
    nr            = 0  # Total number of rows in the table
    i_data        = 0
    nconstraints  = length(tableschema.intrarow_constraints)
    quotechar     = nothing  # In some files values are delimited and quoted. E.g., line = "\"v1\", \"v2\", ...".
    colname2colschema = tableschema.colname2colschema
    f = open(infile)
    for line in eachline(f)
        if !colnames_done
            r = String.(split(line, delim))
            c = Int(r[1][1])  # 1st character of 1st value
            if !(in(c, 48:57) || in(c, 65:90) || in(c, 97:122))
                quotechar = string(r[1][1])
                colnames  = [Symbol(colname) for colname in String.(strip.(split(line, delim), r[1][1]))]
            else
                colnames = [Symbol(colname) for colname in String.(strip.(split(line, delim)))]
            end
            datacols_match_schemacols!(issues, tableschema, Set(colnames))
            colnames_done = true
            continue
        end
        nr += 1
        extract_row!(row, line, delim, colnames, quotechar)  # row = Dict(colname => String(value), ...)
        if length(pk_colnames) > 1 && tableissues[:primarykey_isunique] # Uniqueness of 1-column primary keys is checked at the column level
            populate_primarykey!(primarykey, pk_colnames::Vector{Symbol}, row)
            primarykey_isunique!(tableissues, primarykey, pkvalues)
        end
        parserow!(colname2colschema, row)  # row = Dict(colname => value, ...)
        if enforce  # Write row to outdata
            i_data += 1
            for (colname, val) in row
                ismissing(val) && continue
                !haskey(colname2colschema, colname) && continue
                colschema = colname2colschema[colname]
                if !value_is_valid(val, colschema.validvalues)
                    val = missing
                    row[colname] = missing
                end
                outdata[i_data, colname] = val
            end
            if i_data == n_outdata
                CSV.write(outfile, outdata; append=true)
                i_data = 0  # Reset the row number
            end
        end
        assess_row!(tableissues, columnissues, row, tableschema, uniquevalues)
        length(tableissues[:intrarow_constraints]) == nconstraints && continue     # All constraints have already been breached
        test_intrarow_constraints!(tableissues[:intrarow_constraints], tableschema, row)
    end
    close(f)
    i_data != 0 && CSV.write(outfile, outdata[1:i_data, :]; append=true)
    storeissues(issues, tableissues, columnissues, tablename, nr)
end


################################################################################
# Non-API functions

"Ensure the set of columns in the data matches that in the schema."
function datacols_match_schemacols!(issues, tableschema::TableSchema, colnames_data::Set{Symbol})
    tablename       = String(tableschema.name)
    colnames_schema = Set(tableschema.columnorder)
    cols = setdiff(colnames_data, colnames_schema)
    length(cols) > 0 && push!(issues, (entity="table", id=tablename, issue="The data has columns that the schema doesn't have ($(cols))."))
    cols = setdiff(colnames_schema, colnames_data)
    length(cols) > 0 && push!(issues, (entity="table", id=tablename, issue="The data is missing some columns that the Schema has ($(cols))."))
end

"Values are separated by delim."
function extract_row!(row::Dict{Symbol, Any}, line::String, delim::String, colnames::Vector{Symbol}, quotechar::Nothing)
    i_start = 1
    ncols   = length(colnames)
    for j = 1:ncols
        r = findnext(delim, line, i_start)  # r = i:i, where line[i] == delim
        if isnothing(r)  # If r is nothing then we're in the last column
            row[colnames[j]] = String(strip(line[i_start:end]))
            break
        else
            i_end = r[1] - 1
            row[colnames[j]] = String(strip(line[i_start:i_end]))
            i_start = i_end + 2
        end
    end
end

"Values are separated by delim and quoted by quotechar."
function extract_row!(row::Dict{Symbol, Any}, line::String, delim::String, colnames::Vector{Symbol}, quotechar::String)
    i_start    = 1
    ncols      = length(colnames)
    linelength = length(line)
    for j = 1:ncols
        r = findnext(quotechar, line, i_start)  # r = i:i, where line[i] == stripchar
        isnothing(r) && break
        i_start = r[1] + 1
        r       = findnext(quotechar, line, i_start)
        i_end   = r[1] - 1
        row[colnames[j]] = strip(String(line[i_start:i_end]))
        i_start = r[1] + 1
        i_start > linelength && break
    end
end


"Modified: primarykey"
function populate_primarykey!(primarykey::Vector{String}, pk_colnames::Vector{Symbol}, row)
    j = 0
    for colname in pk_colnames
        j += 1
        primarykey[j] = row[colname]
    end
end


"Modified: tableissues, pkvalues."
function primarykey_isunique!(tableissues, primarykey, pkvalues)
    pk = join(primarykey)
    if in(pk, pkvalues)
        tableissues[:primarykey_isunique] = false
    else
        push!(pkvalues, pk)
    end
end


"""
Modified: row

Parse values from String to colschema.datatype
"""
function parserow!(colname2colschema, row)
    for (colname, colschema) in colname2colschema
        row[colname] = parsevalue(colschema, row[colname])
    end
end

parsevalue(colschema::ColumnSchema, value::Missing) = missing
parsevalue(colschema::ColumnSchema, value::CategoricalValue)  = parsevalue(colschema, get(value))
parsevalue(colschema::ColumnSchema, value::CategoricalString) = parsevalue(colschema, get(value))

function parsevalue(colschema::ColumnSchema, value)
    value == "" && return missing
    value isa colschema.datatype && return value
    try
        parse(colschema.parser, value)
    catch e
        missing
    end
end


function assess_row!(tableissues, columnissues, row, tableschema, uniquevalues)
    tablename = tableschema.name
    for (colname, colschema) in tableschema.colname2colschema
        val = getval(row, colname)
        diagnose_value!(columnissues[colname], val, colschema, uniquevalues, tablename)
    end
end

getval(row::Dict, colname::Symbol) = row[colname]
getval(row,       colname::Symbol) = getproperty(row, colname)


function diagnose_value!(columnissues::Dict{Symbol, Int}, value::T, colschema::ColumnSchema, uniquevalues, tablename) where {T <: CategoricalValue} 
    diagnose_value!(columnissues, get(value), colschema, uniquevalues, tablename)
end


function diagnose_value!(columnissues::Dict{Symbol, Int}, value, colschema::ColumnSchema, uniquevalues, tablename)
    # Ensure no missing data
    if colschema.isrequired && ismissing(value)
        columnissues[:n_missing] += 1
    end

    # Ensure unique data
    if colschema.isunique
        colname = colschema.name
        if in(value, uniquevalues[colname])
            columnissues[:n_notunique] += 1
        elseif !ismissing(value)
            push!(uniquevalues[colname], value)
        end
    end

    # Ensure valid values
    if !ismissing(value) && !value_is_valid(value, colschema.validvalues)
        columnissues[:n_invalid] += 1
    end
end

"""
Modified: constraint_issues.
- row is a property accessible object: val = getproperty(row, colname)
"""
function test_intrarow_constraints!(constraint_issues::Set{String}, tableschema::TableSchema, row::Dict{Symbol, Any})
    for (msg, f) in tableschema.intrarow_constraints
        in(msg, constraint_issues) && continue  # Have already recorded this issue
        ok = @eval $f($row)        # Hack to avoid world age problem.
        ismissing(ok) && continue  # This case is picked up at the column level
        ok && continue
        push!(constraint_issues, msg)
    end
end

"Returns: A table with unpopulated columns with name, type, length and order matching the table schema."
function init_outdata(tableschema::TableSchema, n::Int)
    result = DataFrame()
    for colname in tableschema.columnorder
        colschema = tableschema.colname2colschema[colname]
        result[!, colname] = missings(colschema.datatype, n)
    end
    result
end

"""
Initialises outdata for streaming tables.
Estimates the number of required rows using: filesize = bytes_per_row * nrows
"""
function init_outdata(infile::String, tableschema::TableSchema)
    bytes_per_row = 0
    for (colname, colschema) in tableschema.colname2colschema
        bytes_per_row += colschema.datatype == String ? 30 : 8  # Allow 30 bytes for Strings, 8 bytes for all other data types
    end
    nr = Int(ceil(filesize(infile) / bytes_per_row))
    init_outdata(tableschema, min(nr, 1_000_000))
end

"""
Returns: Issues table, populated, formated and sorted.

Store table issues and column issues in issues, then format and sort.
"""
function storeissues(issues, tableissues, columnissues, tablename, ntotal)
    if !tableissues[:primarykey_isunique]
        push!(issues, (entity="table", id="$(tablename)", issue="Primary key not unique."))
    end
    for msg in tableissues[:intrarow_constraints]
        push!(issues, (entity="table", id="$(tablename)", issue="Intra-row constraint not satisfied: $(msg)"))
    end
    for (colname, d) in columnissues
        store_column_issue!(issues, tablename, colname, d[:n_missing],   ntotal, "have missing data")
        store_column_issue!(issues, tablename, colname, d[:n_notunique], ntotal, "are duplicates")
        store_column_issue!(issues, tablename, colname, d[:n_invalid],   ntotal, "contain invalid values")
    end
    issues = DataFrame(issues)
    sort!(issues, (:entity, :id, :issue), rev=(true, false, false))
end

"Example: 25% (250/1000) of rows contain invalid values."
function store_column_issue!(issues, tablename, colname, num, ntotal, msg_suffix)
    num == 0 && return
    p = make_pct_presentable(100.0 * num / ntotal)
    push!(issues, (entity="column", id="$(tablename).$(colname)", issue="$(p)% ($(num)/$(ntotal)) of rows $(msg_suffix)."))
end

function make_pct_presentable(p)
    p > 1.0 && return "$(round(Int, p))"
    p < 0.1 && return "<0.1"
    "$(round(p; digits=1))"
end

end
