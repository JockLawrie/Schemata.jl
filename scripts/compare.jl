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
output_data_file, input_issues_file, output_issues_file = Schemata.compare_data_to_schema.set_output_files(input_data_file, "", "", "")
println("$(now()) Starting comparison.")
compare(ts, input_data_file; output_data_file=output_data_file, input_issues_file=input_issues_file, output_issues_file=output_issues_file)
println("$(now()) A transformed table has been stored at $(output_data_file).")
println("$(now()) A table of issues with the input data has been stored at $(input_issues_file).")
println("$(now()) A table of issues with the output data has been stored at $(output_issues_file).")
