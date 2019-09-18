module readwrite

export readschema

using YAML
using ..schematypes


function readschema(filename::String)
    # Read yaml
    io = open(filename)
    d  = YAML.load(io)
    close(io)
    length(d) > 1 && error("File $(filename) contains an incorrectly specified schema.")

    # Get schema name
    schema = ""
    schema_name = ""
    for (k, v) in d
        schema_name = k
        schema = v
        break
    end
    schema["name"] = schema_name
    Schema(schema)
end

end
