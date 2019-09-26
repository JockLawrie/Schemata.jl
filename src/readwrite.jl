module readwrite

export readschema

using YAML
using ..types

"Returns either a Schema or a TableSchema, depending on the contents of the config file."
function readschema(filename::String)
    d = YAML.load_file(filename)
    haskey(d, "columns") && return TableSchema(d)  # Config is for a TableSchema
    Schema(d)  # Config is for a Schema
end

end
