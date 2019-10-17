module Schemata

export Schema, TableSchema, ColumnSchema, # types
       compare,                           # core function
       readschema                         # read schema from config file

include("CustomParsers.jl")
include("handle_validvalues.jl")
include("types.jl")
include("compare_table_to_schema.jl")
include("readwrite.jl")

using .CustomParsers
using .handle_validvalues
using .types
using .compare_table_to_schema
using .readwrite

end
