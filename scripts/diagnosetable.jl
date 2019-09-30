#=
  Run this script as follows:
  $ cd /path/to/Schemata.jl
  $ /path/to/julia scripts/diagnosetable.jl /path/to/config.yaml /path/to/data {tablename}
=#

configfile = ARGS[1]
datafile   = ARGS[2]
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
bname, ext     = splitext(basename(datafile))
issues_outfile = joinpath(dirname(datafile), "$(bname)_issues.tsv")
CSV.write(issues_outfile, issues; delim='\t')

# Inform the user
println("A table of issues has been stored at $(issues_outfile).")
