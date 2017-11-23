"""
Returns: table, issues

The table is as compliant as possible with the schema.
If the table is completely compliant with the schema, then the issues table has 0 rows.
Otherwise the issues table contains the ways in which the output table does not comply with the schema.
"""
function enforce_schema(indata, tblschema::TableSchema, set_invalid_to_missing::Bool)
    outdata = init_compliant_data(tblschema, size(indata, 1))
    issues  = Dict{Tuple{String, String}, Set{String}}()  # (entity, id) => Set(issue1, issue2, ...)

    # For each column, for each value:
    # - Parse or convert value if necessary. Record an issue if this is not possible.
    # - If value is invalid and set_invalid_to_missing = true, discard invalid value, else record the (colname,invalid_value) as an issue.
    # - If final value is valid, copy into result.

    # Set categorical columns where required
    for (colname, colschema) in tblschema.columns
        if colschema.is_categorical
            pool!(outdata, colname)
        end
    end

    # Get remaining issues from the output table. Combine these with those found earlier.
    issues = format_issues(issues)
    other_issues = diagnose(outdata, tblschema)
    issues = vcat(issues, other_issues)
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


#=
function add_issue!(issues::Dict{Int, String}, i::String, issue::String)
    !haskey(issues, i) && (issues[i] = Set{String}())
    push!(issues[i], issue)
end


function delete_issue!(issues::Dict{Int, String}, i::String, issue::String)
    pop!(issues[i], issue)
    isempty(issues[i]) && delete!(issues, i)
end
=#
