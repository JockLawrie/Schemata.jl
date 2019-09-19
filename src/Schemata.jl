module Schemata

export Schema, TableSchema, ColumnSchema, # types
       diagnose, enforce_schema,          # core functions
       get_columnschema, insertcolumn!,   # convenience functions
       readschema #writeschema,           # read/write

include("CustomParsers.jl")
include("handle_validvalues.jl")
include("schematypes.jl")
include("diagnose.jl")
include("enforce_schema.jl")
include("conveniences.jl")
include("readwrite.jl")

using .CustomParsers
using .handle_validvalues
using .schematypes
using .diagnosedata
using .enforce
using .conveniences
using .readwrite

end
