module readwrite

export readschema, writeschema

using TOML
using ..types

"Returns either a Schema or a TableSchema, depending on the contents of the config file."
function readschema(filename::String)
    d = TOML.parsefile(filename)
    haskey(d, "columns") && return TableSchema(d)  # Config is for a TableSchema
    Schema(d)  # Config is for a Schema
end

writeschema(outfile::String, schema::Schema)           = toml_to_file(outfile, schema_to_dict(schema))
writeschema(outfile::String, tableschema::TableSchema) = toml_to_file(outfile, tableschema_to_dict(tableschema))

function toml_to_file(outfile::String, d)
    io = open(outfile, "w")
    TOML.print(io, d)
    close(io)
end

################################################################################
# Utils

function schema_to_dict(schema::Schema)
    result = Dict{String, Any}()
    result["name"]        = String(schema.name)
    result["description"] = schema.description
    result["tables"]      = Dict(String(tablename) => tableschema_to_dict(tableschema) for (tablename, tableschema) in schema.tables)
    result
end

function tableschema_to_dict(tableschema::TableSchema)
    result = Dict{String, Any}()
    result["name"]        = String(tableschema.name)
    result["description"] = tableschema.description
    result["primarykey"]  = String.(tableschema.primarykey)
    columns = Dict{String, Any}[]  # colname => colschema
    for colname in tableschema.columnorder
        push!(columns, colschema_to_dict(tableschema.colname2colschema[colname]))
    end
    result["columns"] = columns
    if !isempty(tableschema.intrarow_constraints)
        result["intrarow_constraints"] = Dict(msg => func_as_supplied for (func_as_supplied, f, msg) in tableschema.intrarow_constraints)
    end
    result
end

function colschema_to_dict(colschema::ColumnSchema)
    result = Dict{String, Any}()
    result["name"]          = string(colschema.name)
    result["description"]   = colschema.description
    result["datatype"]      = string(colschema.datatype)
    result["iscategorical"] = colschema.iscategorical
    result["isrequired"]    = colschema.isrequired
    result["isunique"]      = colschema.isunique
    result["validvalues"]   = format_validvalues(colschema.validvalues, colschema.valueorder)
    if !isnothing(colschema.parser_as_supplied)
        result["parser"] = colschema.parser_as_supplied
    end
    result
end

format_validvalues(vv::DataType, valueorder)      = string(vv)
format_validvalues(vv::AbstractRange, valueorder) = string(vv)
format_validvalues(vv::Set, valueorder::Nothing)  = sort!([x for x in vv])
format_validvalues(vv::Set, valueorder::Vector)   = valueorder

end
