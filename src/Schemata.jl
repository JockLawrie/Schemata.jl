module Schemata

export Schema, TableSchema, ColumnSchema, # types
       compare,                           # core function
       readschema #writeschema,           # read/write

include("CustomParsers.jl")
include("handle_validvalues.jl")
include("types.jl")
include("compare_data_to_schema.jl")
include("readwrite.jl")

using .CustomParsers
using .handle_validvalues
using .types
using .compare_data_to_schema
using .readwrite

end
