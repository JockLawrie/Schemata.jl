#=
  Run this script as follows:
  $ cd /path/to/Schemata.jl
  $ /path/to/julia scripts/compare.jl /path/to/config.yaml /path/to/inputdata sorted_by_primarykey
  The 3rd argument, sorted_by_primarykey is either "true" or "false".
  If "true" the compare function assumes that your table is sorted by its primary key,
  which enables a faster comparison to the schema to be made.
=#

configfile      = ARGS[1]
input_data_file = ARGS[2]
sorted_by_primarykey = false
if ARGS[3] == "true"
    sorted_by_primarykey = true
end

using Pkg
Pkg.activate(".")

using Dates
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
compare(ts, input_data_file; sorted_by_primarykey=sorted_by_primarykey)
println("$(now()) A table of transformed data, input issues and output issues have been stored at $(dirname(input_data_file)).")
