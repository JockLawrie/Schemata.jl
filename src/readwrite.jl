function readschema(filename::String)
    # Read yaml
    io  = open(filename)
    dct = YAML.load(io)
    close(io)
    length(dct) > 1 && error("File $(filename) contains an incorrectly specified schema.")

    # Get schema name
    schema = ""
    schema_name = ""
    for (k, v) in dct
        schema_name = k
        schema = v
        break
    end
    schema["name"] = schema_name
    Schema(schema)
end

#=
function writeschema(filename::String, schema::Schema)
end
=#

################################################################################
### Utils
"Returns: An instance of ColumnSchema.valid_values."
function determine_validvalues(vv::String)
    eval(parse("$(module_parent(current_module())).$(vv)"))  # Prepend module for non-Base types
end


function determine_validvalues(vv::Dict)
    vv["type"] = eval(parse("$(module_parent(current_module())).$(vv["type"])"))
    vv
end


function extract_eltype(s::String)
    eval(parse("$(module_parent(current_module())).$(s)"))  # Prepend module for non-Base types
end


function extract_eltype(s::Dict)
    s["type"] = eval(parse("$(module_parent(current_module())).$(s["type"])"))
    s
end
