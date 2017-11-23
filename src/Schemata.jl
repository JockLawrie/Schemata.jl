module Schemata

export CATEGORICAL, IS_REQUIRED, IS_UNIQUE,  # constants
       Schema, TableSchema, ColumnSchema,    # types
       diagnose, enforce_schema              # functions

const CATEGORICAL = true
const IS_REQUIRED = true
const IS_UNIQUE   = true

using DataFrames

include("types.jl")
include("diagnose.jl")
include("enforce_schema.jl")

end
