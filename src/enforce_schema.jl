module enforce

export enforce_schema

using CategoricalArrays
using DataFrames

using ..CustomParsers
using ..handle_validvalues
using ..schematypes
using ..diagnosedata


"""
Returns: table, issues

The returned table is as compliant as possible with the schema.
If the table is completely compliant with the schema, then the issues table has 0 rows.
Otherwise the issues table lists the ways in which the output table does not comply with the schema.
"""
function enforce_schema(indata, tblschema::TableSchema, set_invalid_to_missing::Bool)
    # Init
    n       = size(indata, 1)
    outdata = init_compliant_data(tblschema, n)
    issues  = NamedTuple{(:entity, :id, :issue), Tuple{String,String,String}}[]
    tblname = tblschema.name

    #=
      For each column, for each value:
      - Parse or convert value if necessary. If this is not possible, the value is invalid.
      - If value is invalid and set_invalid_to_missing = true, discard invalid value, else record the (colname,invalid_value) as an issue.
      - If final value is valid, copy into result.
      - Set categorical columns where required
    =#
    for (colname, colschema) in tblschema.columns
        !hasproperty(indata, colname) && continue  # Desired column not in indata; outdata will have a column of missings.
        target_type  = colschema.datatype
        parser       = colschema.parser
        validvals    = colschema.validvalues
        vv_type      = get_datatype(validvals)
        output_type  = nonmissingtype(eltype(outdata[!, colname]))
        invalid_vals = Set{Any}()
        for i = 1:n
            val = indata[i, colname]
            ismissing(val) && continue
            valtype = typeof(val)
            valtype == String && val == "" && continue
            if valtype <: CategoricalString || valtype <: CategoricalValue
                val = get(val)
            end
            is_invalid = false
            if typeof(val) != target_type  # Convert type
                try
                    val = parse(parser, val)
                catch
                    is_invalid = true
                end
            end
            # Value has correct type, now check that value is in the valid range
            if !is_invalid && !value_is_valid(val, validvals)
                is_invalid = true
            end
            # Record invalid value
            if is_invalid && !set_invalid_to_missing
                push!(invalid_vals, val)
            end
            # Write valid value to outdata
            if !is_invalid || (is_invalid && !set_invalid_to_missing)
                if typeof(val) == output_type
                    outdata[i, colname] = val
                end
            end
        end
        if colschema.iscategorical
            categorical!(outdata, colname)
        end
        if !isempty(invalid_vals)
            invalid_vals = [x for x in invalid_vals]  # Convert Set to Vector
            sort!(invalid_vals)
            push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Invalid values: $(invalid_vals)"))
        end
    end

    # Get remaining issues from the output table. Combine these with those found earlier.
    other_issues = diagnose(outdata, tblschema)
    issues = unique(vcat(issues, other_issues))
    outdata, issues
end

"Returns: A table with unpopulated columns with name, type, length and order matching the table schema."
function init_compliant_data(tblschema::TableSchema, n::Int)
    result = DataFrame()
    for colname in tblschema.columnorder
        colschema = tblschema.columns[colname]
        eltyp     = colschema.datatype
        result[!, colname] = missings(eltyp, n)
    end
    result
end

end
