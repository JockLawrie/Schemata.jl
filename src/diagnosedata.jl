module diagnosedata

export diagnose, enforce_schema

using CSV
using DataFrames
using CategoricalArrays
using Tables

using ..CustomParsers
using ..handle_validvalues
using ..schematypes

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
diagnose(table, tableschema::TableSchema) = diagnose_inmemory_table(table, tableschema)

diagnose(datafile::String, tableschema::TableSchema) = diagnose_streaming_table(datafile, tableschema)

enforce_schema(table, tableschema, set_invalid_to_missing) = diagnose_inmemory_table(table, tableschema, true, set_invalid_to_missing)

function enforce_schema(datafile::String, tableschema, set_invalid_to_missing, outfile::String)
    diagnose_streaming_table(datafile, tableschema, true, set_invalid_to_missing, outfile)
end

################################################################################
# diagnose_inmemory_table!

function diagnose_inmemory_table(table, tableschema::TableSchema, enforce::Bool=false, set_invalid_to_missing::Bool=true)
    # Init
    tablename     = tableschema.name
    issues        = NamedTuple{(:entity, :id, :issue), Tuple{String, String, String}}[]
    outdata       = enforce ? init_outdata(tableschema, size(table, 1)) : nothing
    tableissues   = Dict(:primarykey_isunique => true, :intrarow_constraints => Set{String}())
    columnissues  = Dict(col => Dict(:uniqueness_ok => true, :missingness_ok => true, :values_are_valid => true) for col in keys(tableschema.columns))
    colnames      = propertynames(table)
    primarykey    = fill("", length(tableschema.primarykey))
    pk_colnames   = tableschema.primarykey
    pkvalues      = Set{String}()  # Values of the primary key
    uniquevalues  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in tableschema.columns if colschema.isunique==true)
    rowdict       = Dict{Symbol, Any}()
    i_data        = 0
    nconstraints  = length(tableschema.intrarow_constraints)
    colname2colschema = tableschema.columns
    datacols_match_schemacols!(issues, tableschema, Set(colnames))  # Run this on table (not on outdata) so that missing indata columns are reported

    # Row-level checks
    for row in Tables.rows(table)
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
            i_data += 1
            for colname in propertynames(row)      # Copy row to rowdict
                rowdict[colname] = getproperty(row, colname)
            end
            parserow!(colname2colschema, rowdict)  # Parse rowdict
            for (colname, val) in rowdict          # Write rowdict to outdata
                !haskey(colname2colschema, colname) && continue
                if !ismissing(val) && set_invalid_to_missing
                    colschema = colname2colschema[colname]
                    outdata[i_data, colname] = value_is_valid(val, colschema.validvalues) ? val : missing
                else
                    outdata[i_data, colname] = val
                end
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
            !issues_ok(tableissues) && !issues_ok!(columnissues) && break  # All possible issues have been detected
        end
    end

    # Column-level checks
    for (colname, colschema) in tableschema.columns
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
    issues = storeissues(issues, tableissues, columnissues, tablename)
    !enforce && return issues
    outdata, issues
end

################################################################################
# diagnose_streaming_table!

function diagnose_streaming_table(infile::String, tableschema::TableSchema, enforce::Bool=false, set_invalid_to_missing::Bool=true, outfile::String="")
    tablename     = tableschema.name
    issues        = NamedTuple{(:entity, :id, :issue), Tuple{String, String, String}}[]
    outdata       = enforce ? init_outdata(infile, tableschema) : nothing
    n_outdata     = enforce ? size(outdata, 1) : 0
    tableissues   = Dict(:primarykey_isunique => true, :intrarow_constraints => Set{String}())
    columnissues  = Dict(col => Dict(:uniqueness_ok => true, :missingness_ok => true, :values_are_valid => true) for col in keys(tableschema.columns))
    colnames      = nothing
    colnames_done = false
    primarykey    = fill("", length(tableschema.primarykey))
    pk_colnames   = tableschema.primarykey
    pkvalues      = Set{String}()  # Values of the primary key
    uniquevalues  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in tableschema.columns if colschema.isunique==true)
    delim         = infile[(end - 2):end] == "csv" ? "," : "\t"
    row           = Dict{Symbol, Any}()
    i_data        = 0
    nconstraints  = length(tableschema.intrarow_constraints)
    colname2colschema = tableschema.columns
    if enforce
        CSV.write(outfile, init_outdata(tableschema, 0))  # Write column headers to disk
    end
    f = open(infile)
    for line in eachline(f)
        if !colnames_done
            colnames = [Symbol(colname) for colname in strip.(String.(split(line, delim)))]
            datacols_match_schemacols!(issues, tableschema, Set(colnames))
            colnames_done = true
            continue
        end
        extract_row!(row, line, delim, colnames)  # row = Dict(colname => String(value), ...)
        if length(pk_colnames) > 1 && tableissues[:primarykey_isunique] # Uniqueness of 1-column primary keys is checked at the column level
            populate_primarykey!(primarykey, pk_colnames::Vector{Symbol}, row)
            primarykey_isunique!(tableissues, primarykey, pkvalues)
        end
        parserow!(colname2colschema, row)         # row = Dict(colname => value, ...)
        if enforce  # Write row to outdata
            i_data += 1
            for (colname, val) in row
                !haskey(colname2colschema, colname) && continue
                if !ismissing(val) && set_invalid_to_missing
                    colschema = colname2colschema[colname]
                    if !value_is_valid(val, colschema.validvalues)
                        val = missing
                        row[colname] = missing
                    end
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
        !enforce && !issues_ok(tableissues) && !issues_ok!(columnissues) && break  # All possible issues have been detected
    end
    close(f)
    i_data != 0 && CSV.write(outfile, outdata[1:i_data, :]; append=true)
    storeissues(issues, tableissues, columnissues, tablename)
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


function extract_row!(row::Dict{Symbol, Any}, line::String, delim::String, colnames::Vector{Symbol})
    i_start = 1
    colidx  = 0
    for j = 1:10_000  # Maximum of 10_000 columns
        colidx += 1
        r       = findnext(delim, line, i_start)  # r = i:i, where line[i] == '\t'
        if isnothing(r)  # If r is nothing then we're in the last column
            row[colnames[colidx]] = String(line[i_start:end])
            break
        else
            i_end = r[1] - 1
            row[colnames[colidx]] = String(line[i_start:i_end])
            i_start = i_end + 2
        end
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
    for (colname, colschema) in tableschema.columns
        !haskey(columnissues, colname) && continue  # All possible issues for this column have already been found
        val = getval(row, colname)
        diagnose_value!(columnissues[colname], val, colschema, uniquevalues, tablename)
    end
end

getval(row::Dict, colname::Symbol) = row[colname]
getval(row, colname::Symbol)       = getproperty(row, colname)


function diagnose_value!(columnissues::Dict{Symbol, Bool}, value::T, colschema::ColumnSchema, uniquevalues, tablename) where {T <: CategoricalValue} 
    diagnose_value!(columnissues, get(value), colschema, uniquevalues, tablename)
end


function diagnose_value!(columnissues::Dict{Symbol, Bool}, value, colschema::ColumnSchema, uniquevalues, tablename)
    # Ensure no missing data
    if columnissues[:missingness_ok] && colschema.isrequired && ismissing(value)
        columnissues[:missingness_ok] = false
    end

    # Ensure unique data
    if columnissues[:uniqueness_ok] && colschema.isunique
        colname = colschema.name
        if in(value, uniquevalues[colname])
            columnissues[:uniqueness_ok] = false
        elseif !ismissing(value)
            push!(uniquevalues[colname], value)
        end
    end

    # Ensure valid values
    if columnissues[:values_are_valid] && !ismissing(value)
        columnissues[:values_are_valid] = value_is_valid(value, colschema.validvalues)
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


"Returns: True if there are no table-level issues."
function issues_ok(tableissues::Dict{Symbol, Any})
    !tableissues[:primarykey_isunique] && return false
    isempty(tableissues[:intrarow_constraints])
end


"""
Modified: columnissues (key-value pair is removed if the value contains only false).

Return: True if at least 1 column has at least 1 ok.
"""
function issues_ok!(columnissues::Dict{Symbol, Dict{Symbol, Bool}})
    for (colname, d) in columnissues
        n_ok = length(d)
        for (issue, ok) in d
            ok && continue
            n_ok -= 1
        end
        n_ok > 0 && continue
        delete!(columnissues, colname)  # colname has all possible issues...no need to test it on other rows
    end
    length(columnissues) > 0
end


"""
Returns: Issues table, populated, formated and sorted.

Store table issues and column issues in issues, then format and sort.
"""
function storeissues(issues, tableissues, columnissues, tablename)
    if !tableissues[:primarykey_isunique]
        push!(issues, (entity="table", id="$(tablename)", issue="Primary key not unique."))
    end
    for msg in tableissues[:intrarow_constraints]
        push!(issues, (entity="table", id="$(tablename)", issue="Intra-row constraint not satisfied: $(msg)"))
    end
    for (colname, d) in columnissues
        if !d[:missingness_ok]
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="Missing data not allowed."))
        end
        if !d[:uniqueness_ok]
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="Values are not unique."))
        end
        if !d[:values_are_valid]
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="At least 1 value is not valid."))
        end
    end
    issues = DataFrame(issues)
    sort!(issues, (:entity, :id, :issue), rev=(true, false, false))
end

"Returns: A table with unpopulated columns with name, type, length and order matching the table schema."
function init_outdata(tableschema::TableSchema, n::Int)
    result = DataFrame()
    for colname in tableschema.columnorder
        colschema = tableschema.columns[colname]
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
    for (colname, colschema) in tableschema.columns
        bytes_per_row += colschema.datatype == String ? 30 : 8  # Allow 30 bytes for Strings, 8 bytes for all other data types
    end
    nr = Int(ceil(filesize(infile) / bytes_per_row))
    init_outdata(tableschema, min(nr, 1_000_000))
end

end
