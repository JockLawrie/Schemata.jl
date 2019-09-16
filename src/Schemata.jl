module Schemata

export CATEGORICAL, REQUIRED, UNIQUE,     # constants
       Schema, TableSchema, ColumnSchema, # types
       diagnose, enforce_schema,          # functions
       insert_column!,                    # functions
       readschema #writeschema,          # functions

const CATEGORICAL = true
const REQUIRED    = true
const UNIQUE      = true

using Dates
using DataFrames
using TimeZones
using YAML

include("validvalues.jl")
include("types.jl")
include("diagnose.jl")
include("enforce_schema.jl")
include("conveniences.jl")
include("readwrite.jl")

end
