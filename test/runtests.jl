using Test
using Schemata

using CategoricalArrays
using CSV
using DataFrames
using Dates
using TimeZones

const CATEGORICAL = true
const REQUIRED    = true
const UNIQUE      = true

include("test_inmemory_tables.jl")
include("test_ondisk_tables.jl")
