module Schemata

export Schema, TableSchema, ColumnSchema, # types
       diagnose, enforce_schema,          # core functions
       readschema #writeschema,           # read/write

include("CustomParsers.jl")
include("handle_validvalues.jl")
include("types.jl")
include("diagnosedata.jl")
include("readwrite.jl")

using .CustomParsers
using .handle_validvalues
using .types
using .diagnosedata
using .readwrite

end
