module Schemata

export Schema, TableSchema, ColumnSchema, # types
       compare,                           # core function
       readschema, writeschema            # read/write schema from/to config file

include("handle_validvalues.jl")
include("types.jl")
include("compare_table_to_schema.jl")
include("readwrite.jl")

using .handle_validvalues
using .types
using .compare_table_to_schema
using .readwrite

end
