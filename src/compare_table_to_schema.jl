module compare_table_to_schema

export compare

using ..handle_validvalues
using ..types

include("compare/common.jl")
include("compare/inmemory_table.jl")
include("compare/ondisk_table.jl")

using .common
using .inmemory_table
using .ondisk_table


"""
Compares a table to a TableSchema and returns:
- A copy of the input table, transformed as much as possible according to the schema.
- A table of the ways in which the input table doesn't comply with the schema.
- A table of the ways in which the output table doesn't comply with the schema.

There are currently 2 methods for comparing a table to a schema:

1. `compare(table, tableschema)` compares an in-memory table.

2. `compare(tableschema, input_data_file::String; output_data_file="", input_issues_file="", output_issues_file="")` compares a table stored on disk in `input_data_file`.

   This method is designed for tables that are too big for RAM.
   It examines one row at a time.
   The 3 tables of results (see above) are stored on disk. By default they are stored in the same directory as the input table.
"""
compare(tableschema::TableSchema, table; sorted_by_primarykey::Bool=false) = inmemory_table.compare(tableschema, table, sorted_by_primarykey)


function compare(tableschema::TableSchema, input_data_file::String;
                 output_data_file::String="", input_issues_file::String="", output_issues_file::String="", sorted_by_primarykey::Bool=false)
    !isfile(input_data_file) && error("The input data file does not exist.")
    fname, ext = splitext(input_data_file)
    output_data_file   = output_data_file   == "" ? "$(fname)_transformed.tsv"   : output_data_file
    input_issues_file  = input_issues_file  == "" ? "$(fname)_input_issues.tsv"  : input_issues_file
    output_issues_file = output_issues_file == "" ? "$(fname)_output_issues.tsv" : output_issues_file
    outdir  = dirname(output_data_file)  # outdir = "" means output_data_file is in the pwd()
    outdir != "" && !isdir(outdir) && error("The directory containing the specified output file does not exist.")
    ondisk_table.compare(tableschema, input_data_file, output_data_file, input_issues_file, output_issues_file, sorted_by_primarykey)
end

end
