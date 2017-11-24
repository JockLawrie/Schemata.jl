module Schemata

export CATEGORICAL, IS_REQUIRED, IS_UNIQUE,  # constants
       Schema, TableSchema, ColumnSchema,    # types
       readschema, #writeschema,             # functions
       diagnose, enforce_schema,             # functions
       insert_column!                        # functions

const CATEGORICAL = true
const IS_REQUIRED = true
const IS_UNIQUE   = true

using YAML
using DataFrames

include("types.jl")
include("readwrite.jl")
include("diagnose.jl")
include("enforce_schema.jl")

end
