#=
  Run this script as follows:
  $ cd /path/to/Schemata.jl
  $ /path/to/julia scripts/compare.jl /path/to/config.yaml /path/to/inputdata
=#

configfile      = ARGS[1]
input_data_file = ARGS[2]

using Pkg
Pkg.activate(".")

using CSV
using Dates
using DataFrames
using Schemata

# Construct the TableSchema
ts     = nothing
schema = readschema(configfile)
if schema isa Schema
    if isnothing(tablename)
        length(schema.tables) != 1 && error("The schema has more than 1 table. Please specify a table name as a 4th command line argument.")
        for (tablename, tableschema) in schema.tables
            ts = tableschema
            break
        end
    else
        ts = schema.tables[tablename]
    end
elseif schema isa TableSchema
    ts = schema
else
    error("The schema is neither a Schema or a TableSchema.")
end

# Compare data to schema
println("$(now()) Starting comparison.")
compare(ts, input_data_file)
println("$(now()) A table of transformed data, input issues and output issues have been stored at $(dirname(input_data_file)).")