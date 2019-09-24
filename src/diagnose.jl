module diagnosedata

export diagnose

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

################################################################################
# diagnose_inmemory_table!

function diagnose_inmemory_table(table, tableschema::TableSchema)
    # Init
    tablename     = tableschema.name
    issues        = NamedTuple{(:entity, :id, :issue), Tuple{String, String, String}}[]
    tableissues   = Dict(:primarykey_isunique => true, :intrarow_constraints => Set{String}())
    columnissues  = Dict(col => Dict(:uniqueness_ok => true, :missingness_ok => true, :values_are_valid => true) for col in keys(tableschema.columns))
    colnames      = propertynames(table)
    primarykey    = fill("", length(tableschema.primarykey))
    pk_colnames   = tableschema.primarykey
    pkvalues      = Set{String}()  # Values of the primary key
    uniquevalues  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in tableschema.columns if colschema.isunique==true)
    datacols_match_schemacols!(issues, tableschema, Set(colnames))

    # Column-level checks
    for (colname, colschema) in tableschema.columns
        coldata    = getproperty(table, colname)
        data_eltyp = colschema.iscategorical ? eltype(levels(coldata)) : Core.Compiler.typesubtract(eltype(coldata), Missing)
        if data_eltyp != colschema.datatype  # Check data type matches that specified in the ColumnSchema
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="Data has eltype $(data_eltyp), schema requires $(colschema.datatype)."))
        end
        if colschema.iscategorical && !(coldata isa CategoricalVector)  # Ensure categorical values
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="Data is not categorical."))
        end
    end

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
        assess_row!(tableissues, columnissues, row, tableschema, uniquevalues)
        !issues_ok(tableissues) && !issues_ok!(columnissues) && break  # All possible issues have been detected
    end

    # Format result
    storeissues!(issues, tableissues, columnissues, tablename)
    sort!(issues)
end

################################################################################
# diagnose_streaming_table!

function diagnose_streaming_table(tablefile::String, tableschema::TableSchema)
    tablename     = tableschema.name
    issues        = NamedTuple{(:entity, :id, :issue), Tuple{String, String, String}}[]
    tableissues   = Dict(:primarykey_isunique => true, :intrarow_constraints => Set{String}())
    columnissues  = Dict(col => Dict(:uniqueness_ok => true, :missingness_ok => true, :values_are_valid => true) for col in keys(tableschema.columns))
    colnames      = nothing
    colnames_done = false
    primarykey    = fill("", length(tableschema.primarykey))
    pk_colnames   = tableschema.primarykey
    pkvalues      = Set{String}()  # Values of the primary key
    uniquevalues  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in tableschema.columns if colschema.isunique==true)
    delim         = tablefile[(end - 2):end] == "csv" ? "," : "\t"
    row           = Dict{Symbol, Any}()
    colname2colschema = tableschema.columns
    f = open(tablefile)
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
        assess_row!(tableissues, columnissues, row, tableschema, uniquevalues)
        !issues_ok(tableissues) && !issues_ok!(columnissues) && break  # All possible issues have been detected
    end
    close(f)
    storeissues!(issues, tableissues, columnissues, tablename)
    sort!(issues)
end


################################################################################
# Non-API functions

"Ensure the set of columns in the data matches that in the schema."
function datacols_match_schemacols!(issues, tableschema::TableSchema, colnames_data::Set{Symbol})
    tblname         = String(tableschema.name)
    colnames_schema = Set(tableschema.columnorder)
    cols = setdiff(colnames_data, colnames_schema)
    length(cols) > 0 && push!(issues, (entity="table", id=tblname, issue="The data has columns that the schema doesn't have ($(cols))."))
    cols = setdiff(colnames_schema, colnames_data)
    length(cols) > 0 && push!(issues, (entity="table", id=tblname, issue="The data is missing some columns that the Schema has ($(cols))."))
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
    for (colname, colschema) in tableschema.columns
        row[colname] = parsevalue(colschema, row[colname])
    end
end


function parsevalue(colschema::ColumnSchema, value::String)
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
        diagnose_value!(columnissues[colname], getproperty(row, colname), colschema, uniquevalues, tablename)
    end
    length(tableissues[:intrarow_constraints]) == length(tableschema.intrarow_constraints) && return  # All constraints have already been breached
    test_intrarow_constraints!(tableissues[:intrarow_constraints], tableschema, row)
end


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
function test_intrarow_constraints!(constraint_issues::Set{String}, tableschema::TableSchema, row)
    for (msg, f) in tableschema.intrarow_constraints
        in(msg, constraint_issues) && continue  # Have already recorded this issue
        ok = @eval $f($row)        # Hack to avoid world age problem.
        ismissing(ok) && continue  # Missing ok is picked up at the column level
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


function storeissues!(issues, tableissues, columnissues, tablename)
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
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="Values are not valid."))
        end
    end
end

end
