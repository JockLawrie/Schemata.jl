"""
Returns: data, issues
"""
function enforce_schema(indata, schema::TableSchema)
    outdata = init_compliant_data(indata, schema)
    enforce_schema!(outdata, indata, schema)
end






"Returns: A schema-compliant table with dimensions that of indata and cells filled with nulls."
function init_compliant_data(indata, schema::TableSchema)
    outdata = DataFrame()
    ni = size(indata, 1)
    for col in schema.columns
        outdata[col.name] = nulls(col.eltype, ni)
    end
    outdata
end


function enforce_schema!(outdata, indata, schema::TableSchema)
    issues = Dict{Int, Set{String}}()  # row_number => Set([issue1, ...])

    # Convert required fields to Vectors (i.e., null not permitted)

    outdata, issues
end


function add_issue!(issues::Dict{Int, String}, i::String, issue::String)
    !haskey(issues, i) && (issues[i] = Set{String}())
    push!(issues[i], issue)
end


function delete_issue!(issues::Dict{Int, String}, i::String, issue::String)
    pop!(issues[i], issue)
    isempty(issues[i]) && delete!(issues, i)
end
