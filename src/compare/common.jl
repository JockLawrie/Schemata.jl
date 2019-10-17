"""
Contents: Functions for comparing a table to a schema that are used for more than one table type.
"""
module common

export assess_singlecolumn_primarykey!, assess_multicolumn_primarykey!,
       parserow!, assess_row!, assess_missing_value!, assess_nonmissing_value!,
       datacols_match_schemacols!, test_intrarow_constraints!,
       init_outdata, init_issues, construct_issues_table

using DataFrames
using CategoricalArrays
using Tables

using ..CustomParsers
using ..handle_validvalues
using ..types

import Base.getindex
getindex(cr::Tables.ColumnsRow, nm::Symbol) = getproperty(cr, nm)  # For testing intrarow constraints

################################################################################
# Detecting incomplete and/or duplicated primary keys

"Modified: all_issues."
function assess_singlecolumn_primarykey!(all_issues, pk_issues, pk_n_missing::Int, pk_n_notunique::Int)
    pk_incomplete = pk_issues[:n_missing]   > pk_n_missing    # If true then the current inputrow has missing primary key
    pk_duplicated = pk_issues[:n_notunique] > pk_n_notunique  # If true then the current inputrow has duplicated primary key
    if pk_incomplete
        all_issues[:primarykey_incomplete] += 1
    end
    if pk_duplicated
        all_issues[:primarykey_duplicates] += 1
    end
    pk_incomplete, pk_duplicated
end

"Modified: all_issues, primarykey, pkvalues."
function assess_multicolumn_primarykey!(all_issues, primarykey, pk_colnames, pkvalues, sorted_by_primarykey, row)
    if sorted_by_primarykey
        pk_incomplete, pk_duplicated = populate_and_assess_sorted_primarykey!(primarykey, pk_colnames, row)
    else
        pk_incomplete, pk_duplicated = populate_and_assess_unsorted_primarykey!(primarykey, pk_colnames, row, pkvalues)
    end
    if pk_incomplete
        all_issues[:primarykey_incomplete] += 1
    end
    if pk_duplicated
        all_issues[:primarykey_duplicates] += 1
    end
    pk_incomplete, pk_duplicated
end

"""
Modified: pk_prev

Returns: pk_incomplete::Bool, pk_duplicated::Bool.

Used when the input data is sorted by its primary key and the primary key has more than 1 column.

Compares the current row's primary key to the previous row's primary key.
Checks for completeness and duplication.
After doing the checks, pk_prev is populated with the current row's primary key.
"""
function populate_and_assess_sorted_primarykey!(pk_prev::Vector{Union{String, Missing}}, pk_colnames::Vector{Symbol}, currentrow)
    pk_incomplete = false
    pk_duplicated = true
    for (j, colname) in enumerate(pk_colnames)
        val = getproperty(currentrow, colname)
        if ismissing(val)
            pk_incomplete = true
            pk_duplicated = false
        else
            val = string(val)
        end
        @inbounds prev_val = pk_prev[j]
        if ismissing(prev_val) || val != prev_val
            pk_duplicated = false
        end
        @inbounds pk_prev[j] = val
    end
    pk_incomplete, pk_duplicated
end

"""
Modified: primarykey and pkvalues (if the row's primary is complete and hasn't been seen in an eariler row).

Returns: pk_incomplete::Bool, pk_duplicated::Bool.

Used when the input data is not sorted by its primary key and the primary key has more than 1 column.
"""
function populate_and_assess_unsorted_primarykey!(primarykey::Vector{Union{String, Missing}}, pk_colnames::Vector{Symbol}, row, pkvalues)
    pk_incomplete = false
    pk_duplicated = true
    for (j, colname) in enumerate(pk_colnames)
        val = getproperty(row, colname)
        if ismissing(val)
            @inbounds primarykey[j] = missing
            pk_incomplete = true
            pk_duplicated = false
        else
            @inbounds primarykey[j] = string(val)
        end
    end
    if !pk_incomplete
        pkvalue = join(primarykey)
        if !in(pkvalue, pkvalues)  # This pkvalue hasn't already been seen in an earlier row
            push!(pkvalues, pkvalue)
            pk_duplicated = false
        end
    end
    pk_incomplete, pk_duplicated
end

################################################################################
# Converting input values to output values

"""
Modified: outputrow

Parse inputrow into outputrow according to colschema.datatype
"""
function parserow!(outputrow, inputrow, colname2colschema)
    for (colname, colschema) in colname2colschema
        @inbounds outputrow[colname] = parsevalue(colschema, getproperty(inputrow, colname))
    end
end

parsevalue(colschema::ColumnSchema, value::Missing) = missing
parsevalue(colschema::ColumnSchema, value::CategoricalValue)  = parsevalue(colschema, get(value))
parsevalue(colschema::ColumnSchema, value::CategoricalString) = parsevalue(colschema, get(value))


"""
Tries to parse value according to the schema.
Returns missing if parsing is unsuccessful.
"""
function parsevalue(colschema::ColumnSchema, value)
    datatype = colschema.datatype

    # Common specific cases (every line is an early return)
    value == ""         && return missing
    value isa datatype  && return value
    value isa SubString && datatype == String && return String(value)
    if value isa Integer  # Intxx, UIntxx or Bool
        if datatype <: Signed      # Intxx
            value <= typemax(datatype) && return convert(datatype, value)  # Example: convert(Int32, 123)
            return missing
        elseif datatype <: Integer # UIntxx or Bool
            value >= 0 && value <= typemax(datatype) && return convert(datatype, value)  # Example: convert(UInt, 123)
            return missing
        elseif datatype <: AbstractFloat
            value <= typemax(datatype) && return convert(datatype, value)  # Example: convert(Float64, 123)
            return missing
        end
    end
    if value isa AbstractFloat
        if datatype <: Integer
            value != round(value; digits=0) && return missing  # InexactError
            value >= 0.0 && value <= typemax(datatype) && return convert(datatype, value)
            datatype <: Signed && value <= typemax(datatype) && return convert(datatype, value)
            return missing  # value < 0 and datatype <: Unsigned...not possible
        elseif datatype <: AbstractFloat
            value <= typemax(datatype) && return convert(datatype, value)  # Example: convert(Float32, 12.3)
            return missing
        end
    end
    datatype == Char && value isa String && length(value) == 1 && return value[1]

    # General case
    result = (value isa String) && (parentmodule(datatype) == Core) ? Base.tryparse(datatype, value) : nothing
    !isnothing(result) && return result
    result = tryparse(colschema.parser, value)
    isnothing(result) ? missing : result
end


################################################################################
# Cell-level non-compliance issues

"Records issues with the row but doesn't mutate the row."
function assess_row!(columnissues, row, colname2colschema, uniquevalues)
    for (colname, colschema) in colname2colschema
        val = getproperty(row, colname)
        if ismissing(val)
            !colschema.isrequired && continue  # Checks are done before the function call to avoid unnecessary dict lookups
            assess_missing_value!(columnissues[colname])
        elseif value_is_valid(val, colschema.validvalues)
            !colschema.isunique && continue
            assess_nonmissing_value!(columnissues[colname], val, uniquevalues[colname])
        else
            columnissues[colname][:n_invalid] += 1
            !colschema.isunique && continue
            assess_nonmissing_value!(columnissues[colname], val, uniquevalues[colname])
        end
    end
end

assess_missing_value!(columnissues::Dict{Symbol, Int}) = columnissues[:n_missing] += 1  # Ensure no missing data

function assess_nonmissing_value!(columnissues::Dict{Symbol, Int}, value, uniquevalues_colname)
    if in(value, uniquevalues_colname)  # Ensure unique data
        columnissues[:n_notunique] += 1
    else
        push!(uniquevalues_colname, value)
    end
end

function assess_nonmissing_value!(columnissues::Dict{Symbol, Int}, value::T, uniquevalues_colname) where {T <: CategoricalValue} 
    assess_nonmissing_value!(columnissues, get(value), uniquevalues_colname)
end

function assess_nonmissing_value!(columnissues::Dict{Symbol, Int}, value::T, uniquevalues_colname) where {T <: CategoricalString} 
    assess_nonmissing_value!(columnissues, get(value), uniquevalues_colname)
end

################################################################################
# Column-level non-compliance issues

"Ensure the set of columns in the data matches that in the schema."
function datacols_match_schemacols!(issues, tableschema::TableSchema, colnames_data::Set{Symbol})
    tablename       = String(tableschema.name)
    colnames_schema = Set(tableschema.columnorder)
    cols = setdiff(colnames_data, colnames_schema)
    if length(cols) > 0
        issues[:data_extra_cols] = sort!([x for x in cols])
    end
    cols = setdiff(colnames_schema, colnames_data)
    if length(cols) > 0
        issues[:data_missing_cols] = sort!([x for x in cols])
    end
end

################################################################################
# Detecting for breaches of intra-row constraints

"""
Modified: constraint_issues.
"""
function test_intrarow_constraints!(constraint_issues::Dict{String, Int}, tableschema::TableSchema, row)
    for (msg, f) in tableschema.intrarow_constraints
        ok = @eval $f($row)        # Hack to avoid world age problem.
        ismissing(ok) && continue  # This case is picked up at the column level
        ok && continue
        if haskey(constraint_issues, msg)
            constraint_issues[msg] += 1
        else
            constraint_issues[msg] = 1
        end
    end
end

################################################################################
# Initialise output data

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
function init_outdata(tableschema::TableSchema, input_data_file::String)
    bytes_per_row = 0
    for (colname, colschema) in tableschema.colname2colschema
        bytes_per_row += colschema.datatype == String ? 30 : 8  # Allow 30 bytes for Strings, 8 bytes for all other data types
    end
    nr = Int(ceil(filesize(input_data_file) / bytes_per_row))
    init_outdata(tableschema, min(nr, 1_000_000))
end

################################################################################
# Reporting non-compliance issues

function init_issues(tableschema::TableSchema)
    result = Dict(:primarykey_duplicates => 0, :primarykey_incomplete => 0, :data_extra_cols => Symbol[], :data_missing_cols => Symbol[])
    result[:intrarow_constraints] = Dict{String, Int}()
    result[:columnissues] = Dict{Symbol, Dict{Symbol, Int}}()
    for (colname, colschema) in tableschema.colname2colschema
        d = Dict(:n_notunique => 0, :n_missing => 0, :n_invalid => 0, :different_datatypes => 0, :data_not_categorical => 0, :data_is_categorical => 0)
        result[:columnissues][colname] = d
    end
    result
end

"""
Returns: Converts issues object to a formatted and sorted DataFrame.
"""
function construct_issues_table(issues, tableschema, ntotal)
    result    = NamedTuple{(:entity, :id, :issue), Tuple{String, String, String}}[]
    tablename = tableschema.name
    if !isempty(issues[:data_extra_cols])
        push!(result, (entity="table", id="$(tablename)", issue="The data has columns that the schema doesn't have ($(issues[:data_extra_cols]))."))
    end
    if !isempty(issues[:data_missing_cols])
        push!(result, (entity="table", id="$(tablename)", issue="The data is missing some columns that the Schema has ($(issues[:data_missing_cols]))."))
    end
    if issues[:primarykey_incomplete] > 0
        num = issues[:primarykey_incomplete]
        p   = make_pct_presentable(100.0 * num / ntotal)
        push!(result, (entity="table", id="$(tablename)", issue="$(p)% ($(num)/$(ntotal)) of rows contain missing values in the primary key."))
    end
    if issues[:primarykey_duplicates] > 0
        nd = issues[:primarykey_duplicates]
        p  = make_pct_presentable(100.0 * nd / ntotal)
        push!(result, (entity="table", id="$(tablename)", issue="$(p)% ($(nd)/$(ntotal)) of rows contain duplicated values of the primary key."))
    end
    for (constraint, nr) in issues[:intrarow_constraints]
        p = make_pct_presentable(100.0 * nr / ntotal)
        msg = "$(p)% ($(nr)/$(ntotal)) of rows don't satisfy the constraint: $(constraint)"
        push!(result, (entity="table", id="$(tablename)", issue=msg))
    end
    for (colname, colschema) in tableschema.colname2colschema
        !haskey(issues[:columnissues], colname) && continue
        d = issues[:columnissues][colname]
        if d[:different_datatypes] > 0
            push!(result, (entity="column", id="$(tablename).$(colname)", issue="The schema requires the data to have type $(colschema.datatype)."))
        end
        if d[:data_not_categorical] > 0
            push!(result, (entity="column", id="$(tablename).$(colname)", issue="The data is not categorical."))
        end
        if d[:data_is_categorical] > 0
            push!(issues, (entity="column", id="$(tablename).$(colname)", issue="The data is categorical but shouldn't be."))
        end
        store_column_issue!(result, tablename, colname, d[:n_missing],   ntotal, "have missing data")
        store_column_issue!(result, tablename, colname, d[:n_notunique], ntotal, "are duplicates")
        store_column_issue!(result, tablename, colname, d[:n_invalid],   ntotal, "contain invalid values")
    end
    result = DataFrame(result)
    sort!(result, (:entity, :id, :issue), rev=(true, false, false))
end

"Example: 25% (250/1000) of rows contain invalid values."
function store_column_issue!(issues, tablename, colname, num, ntotal, msg_suffix)
    num == 0 && return
    p = make_pct_presentable(100.0 * num / ntotal)
    push!(issues, (entity="column", id="$(tablename).$(colname)", issue="$(p)% ($(num)/$(ntotal)) of rows $(msg_suffix)."))
end

function make_pct_presentable(p)
    p > 100.0 && return "100"
    p > 1.0   && return "$(round(Int, p))"
    p < 0.1   && return "<0.1"
    "$(round(p; digits=1))"
end

end
