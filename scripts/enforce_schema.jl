#=
  Run this script as follows:
  $ cd /path/to/Schemata.jl
  $ /path/to/julia scripts/enforece_schema.jl /path/to/config.yaml /path/to/data {tablename}
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

# Enforce schema
bname, ext   = splitext(basename(datafile))
data_outfile = joinpath(dirname(datafile), "$(bname)_transformed$(ext)")
issues = enforce_schema(datafile, ts, data_outfile)

# Write output to disk
issues_outfile = joinpath(dirname(datafile), "$(bname)_remaining_issues.tsv")
CSV.write(issues_outfile, issues; delim='\t')

# Inform the user
println("A transformed table has been stored at $(data_outfile).")
println("A table of remaining issues has been stored at $(issues_outfile).")
