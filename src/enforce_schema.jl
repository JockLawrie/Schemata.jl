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
        target_type  = colschema.eltyp
        validvals    = colschema.valid_values
        vv_type      = typeof(validvals)
        invalid_vals = Set{Any}()
        for i = 1:n
            val = indata[i, colname]
            ismissing(val) && continue
            is_invalid = false
            if typeof(val) != target_type  # Convert type
                try
                    val = parse_as_type(target_type, val)
                catch
                    is_invalid = true
                end
            end
            # Value has correct type, now check that value is in the valid range
            if !is_invalid && (vv_type <: Vector || vv_type <: Range) && !value_is_valid(val, validvals)
                is_invalid = true
            end



            # Record invalid value
            if is_invalid && !set_invalid_to_missing
                push!(invalid_vals, val)
            end
            if !is_invalid || (is_invalid && !set_invalid_to_missing)
                if typeof(val) == Missings.T(eltype(outdata[colname]))
                    outdata[i, colname] = val
                end
            end
        end
        if colschema.is_categorical
            categorical!(outdata, colname)
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

value_is_valid(val, validvals::Vector) = in(val, validvals) ? true : false
value_is_valid(val, validvals::Range)  = val >= validvals[1] && val <= validvals[end]  #Check only the end points for efficiency. TODO: Check interior points efficiently.


"Returns: A table with unpopulated columns with name, type, length and order matching the table schema."
function init_compliant_data(tblschema::TableSchema, n::Int)
    result = DataFrame()
    for colname in tblschema.col_order
        colschema = tblschema.columns[colname]
        eltyp     = eltype(colschema)
        result[colname] = missings(eltyp, n)
    end
    result
end


parse_as_type(target_type, val::String) = parse(target_type, val)

parse_as_type(target_type, val) = convert(target_type, val)

function parse_as_type(target_type::Dict, val::String)
    if haskey(target_type, "kwargs")
        target_type["type"](val, target_type["args"]...; target_type["kwargs"]...)
    else
        target_type["type"](val, target_type["args"]...)
    end
end
