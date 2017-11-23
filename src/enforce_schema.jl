#=
function enforce_schema!(indata, tblschema::TableSchema, set_invalid_to_missing::Bool)
    # init: convert columns of indata where possible, ensure column order


    issues
end
=#


"""
Returns: table, issues

The table is as compliant as possible with the schema.
If the table is completely compliant with the schema, then the issues table has 0 rows.
Otherwise the issues table contains the ways in which the output table does not comply with the schema.
"""
function enforce_schema(indata, tblschema::TableSchema, set_invalid_to_missing::Bool)
    n       = size(indata, 1)
    outdata = init_compliant_data(tblschema, n)
    issues  = Dict{Tuple{String, String}, Set{String}}()  # (entity, id) => Set(issue1, issue2, ...)
    tblname = tblschema.name

    #=
      For each column, for each value:
      - Parse or convert value if necessary. If this is not possible, the value is invalid.
      - If value is invalid and set_invalid_to_missing = true, discard invalid value, else record the (colname,invalid_value) as an issue.
      - If final value is valid, copy into result.
      - Set categorical columns where required
    =#
    for (colname, colschema) in tblschema.columns
        target_type  = eltype(outdata[colname])
        validvals    = colschema.valid_values
        vv_type      = typeof(validvals)
        invalid_vals = Set{Any}()
        for i = 1:n
            val = indata[i, colname]
            isna(val) && continue
            is_invalid = false
            if typeof(val) != target_type  # Convert type
                try
                    val = typeof(val) == String ? parse(target_type, val) : convert(target_type, val)
                catch
                    is_invalid = true
                end
            end
            if !is_invalid && (vv_type <: Vector || vv_type <: Range)  # Check whether value is valid
                if !in(val, validvals)
                    is_invalid = true
                end
            end
            if is_invalid && !set_invalid_to_missing  # Record invalid value
                push!(invalid_vals, val)
            end
            if !is_invalid || (is_invalid && !set_invalid_to_missing)
                if typeof(val) == target_type
                    outdata[i, colname] = val
                end
            end
        end
        if colschema.is_categorical
            pool!(outdata, colname)
        end
        if !isempty(invalid_vals)
            invalid_vals = [x for x in invalid_vals]  # Convert Set to Vector
            sort!(invalid_vals)
            insert_issue!(issues, ("column", "$tblname.$colname"), "Invalid values: $(invalid_vals)")
        end
    end

    # Get remaining issues from the output table. Combine these with those found earlier.
    issues = format_issues(issues)
    other_issues = diagnose(outdata, tblschema)
    issues = unique(vcat(issues, other_issues))
    outdata, issues
end


"Returns: A table with unpopulated columns with name, type, length and order matching the table schema."
function init_compliant_data(tblschema::TableSchema, n::Int)
    result = DataFrame()
    for colname in tblschema.col_order
        colschema = tblschema.columns[colname]
        result[colname] = DataArray(colschema.eltyp, n)
    end
    result
end
