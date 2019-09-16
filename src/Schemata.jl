module Schemata

export CATEGORICAL, REQUIRED, UNIQUE,     # constants
       Schema, TableSchema, ColumnSchema, # types
       readschema, #writeschema,          # functions
       diagnose, enforce_schema,          # functions
       insert_column!                     # functions

const CATEGORICAL = true
const REQUIRED    = true
const UNIQUE      = true

using Dates
using DataFrames
using TimeZones
using YAML

include("types.jl")
include("readwrite.jl")
include("diagnose.jl")
include("enforce_schema.jl")
include("conveniences.jl")

end
