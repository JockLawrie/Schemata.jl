#=
  Run this script as follows:
  $ cd /path/to/Schemata.jl
  $ julia /path/to/this/script /path/to/data`/path/to/config.yaml {tablename}
=#

datafile   = ARGS[1]
configfile = ARGS[2]
tablename  = length(ARGS) == 3 ? ARGS[3] : nothing

using Pkg
Pkg.activate(".")

using CSV
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

# Diagnose
issues = diagnose(datafile, ts)

# Write output to disk
issues_outfile = joinpath(dirname(datafile), "issues.tsv")
CSV.write(issues_outfile, issues)

# Inform the user
println("A table of issues has been stored at $(issues_outfile).")
