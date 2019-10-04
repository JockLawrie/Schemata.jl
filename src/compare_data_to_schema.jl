module compare_data_to_schema

export compare

using CSV
using DataFrames
using CategoricalArrays
using Tables

using ..CustomParsers
using ..handle_validvalues
using ..types


import Base.getindex
getindex(r::CSV.Row2, nm::Symbol) = getproperty(r, nm)
getindex(cr::Tables.ColumnsRow, nm::Symbol) = getproperty(cr, nm)

################################################################################
# API function

"""
Compares a table to a TableSchema and produces:
- A copy of the input table, transformed as much as possible according to the schema.
- A table of the ways in which the input table doesn't comply with the schema.
- A table of the ways in which the output table doesn't comply with the schema.

There are 2 methods for comparing a table to a schema:

1. `compare(table, tableschema)` compares an in-memory table.

2. `compare(tableschema, input_data_file::String; output_data_file="", input_issues_file="", output_issues_file="")` compares a table stored on disk in `input_data_file`.

   This method is designed for tables that are too big for RAM.
   It examines one row at a time.
   The 3 tables of results (see above) are stored on disk. By default they are stored in the same directory as the input table.
"""
compare(tableschema::TableSchema, table) = compare_inmemory_table(tableschema, table)

function compare(tableschema::TableSchema, input_data_file::String; output_data_file="", input_issues_file="", output_issues_file="")
    !isfile(input_data_file) && error("The input data file does not exist.")
    fname, ext = splitext(input_data_file)
    output_data_file   = output_data_file   == "" ? "$(fname)_transformed.tsv"   : output_data_file
    input_issues_file  = input_issues_file  == "" ? "$(fname)_input_issues.tsv"  : input_issues_file
    output_issues_file = output_issues_file == "" ? "$(fname)_output_issues.tsv" : output_issues_file
    outdir  = dirname(output_data_file)  # outdir = "" means output_data_file is in the pwd()
    outdir != "" && !isdir(outdir) && error("The directory containing the specified output file does not exist.")
    compare_ondisk_table(tableschema, input_data_file, output_data_file, input_issues_file, output_issues_file)
end

################################################################################
# compare_inmemory_table

function compare_inmemory_table(tableschema::TableSchema, indata)
    # Init
    tablename     = tableschema.name
    outdata       = init_outdata(tableschema, size(indata, 1))
    issues_in     = init_issues(tableschema)  # Issues for indata
    issues_out    = init_issues(tableschema)  # Issues for outdata
    pk_colnames   = tableschema.primarykey
    primarykey    = fill("", length(pk_colnames))  # Stringified primary key
    pkvalues_in   = Set{String}()  # Values of the primary key
    pkvalues_out  = Set{String}()
    i_outdata     = 0
    nconstraints  = length(tableschema.intrarow_constraints)
    colname2colschema = tableschema.colname2colschema
    uniquevalues_in   = Dict(colname => Set{nonmissingtype(eltype(getproperty(indata, colname)))}() for (colname, colschema) in colname2colschema if colschema.isunique==true)
    uniquevalues_out  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in colname2colschema if colschema.isunique==true)

    # Row-level checks
    for inputrow in Tables.rows(indata)
        # Parse inputrow into outputrow according to ColumnSchema
        i_outdata += 1
        outputrow  = outdata[i_outdata, :]
        parserow!(outputrow, inputrow, colname2colschema)

        # Assess input row
        if length(pk_colnames) > 1  # The uniqueness of 1-column primary keys is checked at the column level
            populate_primarykey!(primarykey, pk_colnames, inputrow)
            pkvalue = join(primarykey)
            primarykey_isunique!(issues_in, pkvalues_in, pkvalue)
        end
        assess_row!(issues_in[:columnissues], inputrow, colname2colschema, uniquevalues_in)
        nconstraints > 0 && test_intrarow_constraints!(issues_in[:intrarow_constraints], tableschema, inputrow)

        # Assess output row
        assess_row_mutate!(issues_out[:columnissues], outputrow, colname2colschema, uniquevalues_out)
        if length(pk_colnames) > 1
            populate_primarykey!(primarykey, pk_colnames, outputrow)
            pkvalue = join(primarykey)
            primarykey_isunique!(issues_out, pkvalues_out, pkvalue)
        end
        nconstraints > 0 && test_intrarow_constraints!(issues_out[:intrarow_constraints], tableschema, outputrow)
    end

    # Column-level checks
    for (colname, colschema) in colname2colschema
        !colschema.iscategorical && continue
        categorical!(outdata, colname)
    end
    datacols_match_schemacols!(issues_in, tableschema, Set(propertynames(indata)))  # By construction this issue doesn't exist for outdata
    compare_datatypes!(issues_in,  indata,  colname2colschema)
    compare_datatypes!(issues_out, outdata, colname2colschema)

    # Format result
    issues_in  = construct_issues_table(issues_in,  tableschema, i_outdata)
    issues_out = construct_issues_table(issues_out, tableschema, i_outdata)
    outdata, issues_in, issues_out
end

################################################################################
# compare_ondisk_table

function compare_ondisk_table(tableschema::TableSchema,
                              input_data_file::String, output_data_file::String, input_issues_file::String, output_issues_file::String)
    # Init
    tablename     = tableschema.name
    outdata       = init_outdata(tableschema, input_data_file)
    issues_in     = init_issues(tableschema)  # Issues for indata
    issues_out    = init_issues(tableschema)  # Issues for outdata
    pk_colnames   = tableschema.primarykey
    primarykey    = fill("", length(pk_colnames))  # Stringified primary key 
    pkvalues_in   = Set{String}()  # Values of the primary key
    pkvalues_out  = Set{String}()
    i_outdata     = 0
    nconstraints  = length(tableschema.intrarow_constraints)
    colname2colschema = tableschema.colname2colschema
    uniquevalues_in   = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in colname2colschema if colschema.isunique==true)
    uniquevalues_out  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in colname2colschema if colschema.isunique==true)

    nr            = 0  # Total number of rows in the output data
    n_outdata     = size(outdata, 1)
    delim_outdata = output_data_file[(end - 2):end] == "csv" ? "," : "\t"
    delim_iniss   = input_issues_file[(end - 2):end] == "csv" ? "," : "\t"
    delim_outiss  = output_issues_file[(end - 2):end] == "csv" ? "," : "\t"
    quotechar     = nothing  # In some files values are delimited and quoted. E.g., line = "\"v1\", \"v2\", ...".
    colissues_in  = issues_in[:columnissues]
    colissues_out = issues_out[:columnissues]
    CSV.write(output_data_file, init_outdata(tableschema, 0); delim=delim_outdata)  # Write column headers to disk
    csvrows = CSV.Rows(input_data_file; reusebuffer=true)
    for inputrow in csvrows
        # Parse inputrow into outputrow according to ColumnSchema
        i_outdata += 1
        outputrow  = outdata[i_outdata, :]
        parserow!(outputrow, inputrow, colname2colschema)

        # Assess input row
        if length(pk_colnames) > 1  # The uniqueness of 1-column primary keys is checked at the column level
            populate_primarykey!(primarykey, pk_colnames, inputrow)
            pkvalue = join(primarykey)
            primarykey_isunique!(issues_in, pkvalues_in, pkvalue)
        end
        assess_row!(colissues_in, outputrow, colname2colschema, uniquevalues_in)  # Note: outputrow used because inputrow contains only Strings
        nconstraints > 0 && test_intrarow_constraints!(issues_in[:intrarow_constraints], tableschema, outputrow)

        # Assess output row
        # For speed, avoid testing value_is_valid directly. Instead reuse assessment of input.
        # Testing intra-row constraints is unnecessary because either outputrow hasn't changed or the tests return early due to missingness
        ischanged = false  # True if at least 1 value in outputrow is changed to missing
        for (colname, colschema) in colname2colschema
            val = outputrow[colname]
            ci  = colissues_out[colname]
            if colissues_in[colname][:n_invalid] == ci[:n_invalid]  # input value (=output value) is valid...no change to outputrow
                if ismissing(val)
                    if colschema.isrequired
                        ci[:n_missing] += 1
                    end
                else
                    if colschema.isunique
                        if in(val, uniquevalues_out[colname])
                            ci[:n_notunique] += 1
                        else
                            push!(uniquevalues_out[colname], val)
                        end
                    end
                end
            else  # input value (=output value) is invalid...set to missing and report as missing, not as invalid
                ischanged = true
                @inbounds outputrow[colname] = missing
                if colschema.isrequired
                    ci[:n_missing] += 1
                end
            end
        end
        if length(pk_colnames) > 1  # Single-column primary keys are assessed via columnissues[colname][:n_notunique]
            if ischanged
                populate_primarykey!(primarykey, pk_colnames, outputrow)  # Only necessary if outputrow has changed
                pkvalue = join(primarykey)
            end
            primarykey_isunique!(issues_out, pkvalues_out, pkvalue)
        end

        # If outdata is full append it to output_data_file
        i_outdata != n_outdata && continue
        i_outdata  = 0  # Reset the row number
        nr        += n_outdata
        CSV.write(output_data_file, outdata; append=true, delim=delim_outdata)
    end
    if i_outdata != 0
        CSV.write(output_data_file, view(outdata, 1:i_outdata, :); append=true, delim=delim_outdata)
        nr += i_outdata
    end

    # Column-level checks
    datacols_match_schemacols!(issues_in, tableschema, Set(csvrows.names))  # By construction this issue doesn't exist for outdata

    # Format result
    issues_in  = construct_issues_table(issues_in,  tableschema, nr)
    issues_out = construct_issues_table(issues_out, tableschema, nr)
    CSV.write(input_issues_file,  issues_in;  delim=delim_iniss)
    CSV.write(output_issues_file, issues_out; delim=delim_outiss)
end


################################################################################
# Non-API functions

function init_issues(tableschema::TableSchema)
    result = Dict(:primarykey_duplicates => 0, :intrarow_constraints => Dict{String, Int}(), :data_extra_cols => Symbol[], :data_missing_cols => Symbol[])
    result[:columnissues] = Dict{Symbol, Dict{Symbol, Int}}()
    for (colname, colschema) in tableschema.colname2colschema
        d = Dict(:n_notunique => 0, :n_missing => 0, :n_invalid => 0, :different_datatypes => 0, :data_not_categorical => 0, :data_is_categorical => 0)
        result[:columnissues][colname] = d
    end
    result
end

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

"""
Modified: issues.

Checks whether each column and its schema are both categorical or both not categorical, and whether they have the same data types.
"""
function compare_datatypes!(issues, table, colname2colschema)
    for (colname, colschema) in colname2colschema
        coldata    = getproperty(table, colname)
        data_eltyp = colschema.iscategorical ? eltype(levels(coldata)) : nonmissingtype(eltype(coldata))
        if data_eltyp != colschema.datatype  # Check data type matches that specified in the ColumnSchema
            issues[:columnissues][colname][:different_datatypes] = 1
        end
        if colschema.iscategorical && !(coldata isa CategoricalVector)  # Ensure categorical values
            issues[:columnissues][colname][:data_not_categorical] = 1
        end
        if !colschema.iscategorical && coldata isa CategoricalVector    # Ensure non-categorical values
            issues[:columnissues][colname][:data_is_categorical] = 1
        end
    end
end

"Modified: primarykey"
function populate_primarykey!(primarykey::Vector{String}, pk_colnames::Vector{Symbol}, row)
    j = 0
    for colname in pk_colnames
        j += 1
        primarykey[j] = string(row[colname])
    end
end

"Modified: Either issues or pkvalues."
function primarykey_isunique!(issues, pkvalues, pkvalue)
    if in(pkvalue, pkvalues)  # This pkvalue has already been seen in an earlier row
        issues[:primarykey_duplicates] += 1
    else
        push!(pkvalues, pkvalue)
    end
end

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

function parsevalue(colschema::ColumnSchema, value)
    value == "" && return missing
    value isa colschema.datatype && return value
    try
        parse(colschema.parser, value)
    catch e
        try
            convert(colschema.datatype, value)
        catch e2
            missing
        end
    end
end

function assess_row!(columnissues, outputrow, colname2colschema, uniquevalues)
    for (colname, colschema) in colname2colschema
        diagnose_value!(columnissues[colname], outputrow[colname], colschema, uniquevalues, false)
    end
end

function assess_row_mutate!(columnissues, outputrow, colname2colschema, uniquevalues)
    for (colname, colschema) in colname2colschema
        val = diagnose_value!(columnissues[colname], outputrow[colname], colschema, uniquevalues, true)
        if ismissing(val)
            outputrow[colname] = missing
        end
    end
end

function diagnose_value!(columnissues::Dict{Symbol, Int}, value::T, colschema::ColumnSchema,
                         uniquevalues, set_invalid_to_missing) where {T <: CategoricalValue} 
    diagnose_value!(columnissues, get(value), colschema, uniquevalues, set_invalid_to_missing)
end

function diagnose_value!(columnissues::Dict{Symbol, Int}, value::T, colschema::ColumnSchema,
                         uniquevalues, set_invalid_to_missing) where {T <: CategoricalString} 
    diagnose_value!(columnissues, get(value), colschema, uniquevalues, set_invalid_to_missing)
end

function diagnose_value!(columnissues::Dict{Symbol, Int}, value, colschema::ColumnSchema, uniquevalues, set_invalid_to_missing)
    # Ensure valid values
    if !ismissing(value) && !value_is_valid(value, colschema.validvalues)
        if set_invalid_to_missing  # Only applied to outdata (not indata)
            value = missing
        else
            columnissues[:n_invalid] += 1
        end
    end

    # Ensure no missing data
    if colschema.isrequired && ismissing(value)
        columnissues[:n_missing] += 1
    end

    # Ensure unique data
    if colschema.isunique && !ismissing(value)
        if in(value, uniquevalues[colschema.name])
            columnissues[:n_notunique] += 1
        else
            push!(uniquevalues[colschema.name], value)
        end
    end
    value
end

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
