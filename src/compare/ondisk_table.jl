"""
Compare an on-disk table to a table schema.
"""
module ondisk_table

using CSV
using DataFrames

using ..types
using ..common

function compare(tableschema::TableSchema, input_data_file::String, output_data_file::String,
                 input_issues_file::String, output_issues_file::String, sorted_by_primarykey::Bool)
    # Init
    tablename     = tableschema.name
    outdata       = init_outdata(tableschema, input_data_file)
    issues_in     = init_issues(tableschema)  # Issues for indata
    issues_out    = init_issues(tableschema)  # Issues for outdata
    pk_colnames   = tableschema.primarykey
    pk_ncols      = length(pk_colnames)
    primarykey    = missings(String, pk_ncols)  # Stringified primary key 
    pkvalues_in   = Set{String}()  # Values of the primary key
    pk_issues_in  = pk_ncols == 1 ? issues_in[:columnissues][pk_colnames[1]]  : Dict{Symbol, Int}()
    i_outdata     = 0
    nconstraints  = length(tableschema.intrarow_constraints)
    pk_colnames_set   = Set(pk_colnames)
    colname2colschema = tableschema.colname2colschema
    uniquevalues_in   = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in colname2colschema if colschema.isunique==true)
    uniquevalues_out  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in colname2colschema if colschema.isunique==true)

    nr            = 0  # Total number of rows in the output data
    n_outdata     = size(outdata, 1)
    delim_outdata = output_data_file[(end - 2):end] == "csv" ? "," : "\t"
    delim_iniss   = input_issues_file[(end - 2):end] == "csv" ? "," : "\t"
    delim_outiss  = output_issues_file[(end - 2):end] == "csv" ? "," : "\t"
    quotechar     = nothing  # In some files values are delimited and quoted. E.g., line = "\"v1\", \"v2\", ...".
    colissues_in  = issues_in[:columnissues]
    colissues_out = issues_out[:columnissues]
    CSV.write(output_data_file, init_outdata(tableschema, 0); delim=delim_outdata)  # Write column headers to disk
    csvrows = CSV.Rows(input_data_file; reusebuffer=true, use_mmap=true)
    for inputrow in csvrows
        # Parse inputrow into outputrow according to ColumnSchema
        i_outdata += 1
        outputrow  = outdata[i_outdata, :]
        parserow!(outputrow, inputrow, colname2colschema)

        # Assess input row
        if pk_ncols == 1
            pk_n_missing   = pk_issues_in[:n_missing]    # Number of earlier rows (excluding the curent row) with missing primary key
            pk_n_notunique = pk_issues_in[:n_notunique]  # Number of earlier rows (excluding the curent row) with duplicated primary key
        end
        assess_row!(colissues_in, outputrow, colname2colschema, uniquevalues_in)  # Note: outputrow used because inputrow contains only Strings
        nconstraints > 0 && test_intrarow_constraints!(issues_in[:intrarow_constraints], tableschema, outputrow)
        if pk_ncols == 1
            pk_incomplete, pk_duplicated = assess_singlecolumn_primarykey!(issues_in, pk_issues_in, pk_n_missing, pk_n_notunique)
        else
            pk_incomplete, pk_duplicated = assess_multicolumn_primarykey!(issues_in, primarykey, pk_colnames, pkvalues_in, sorted_by_primarykey, outputrow)
        end

        # Assess output row
        # For speed, avoid testing value_is_valid directly. Instead reuse assessment of input.
        # Testing intra-row constraints is unnecessary because either outputrow hasn't changed or the tests return early due to missingness
        pk_has_changed = false
        for (colname, colschema) in colname2colschema
            val = outputrow[colname]
            ci  = colissues_out[colname]
            if colissues_in[colname][:n_invalid] == ci[:n_invalid]  # input value (=output value) is valid...no change to outputrow
                if ismissing(val)
                    if colschema.isrequired
                        ci[:n_missing] += 1
                    end
                else
                    if colschema.isunique
                        uniquevals = uniquevalues_out[colname]
                        if in(val, uniquevals)
                            ci[:n_notunique] += 1
                        else
                            push!(uniquevals, val)
                        end
                    end
                end
            else  # input value (=output value) is invalid...set to missing and report as missing, not as invalid
                @inbounds outputrow[colname] = missing
                pk_has_changed = pk_ncols > 1 && !pk_has_changed && in(colname, pk_colnames_set)
                ci[:n_invalid] += 1  # Required for comparison between input and output. Reset to 0 below.
                if colschema.isrequired
                    ci[:n_missing] += 1
                end
            end
        end
        if pk_has_changed || pk_incomplete
            issues_out[:primarykey_incomplete] += 1
        elseif pk_duplicated
            issues_out[:primarykey_duplicates] += 1  # Output pk is valid and equals input pk, which is duplicated (and therefore complete)
        end

        # If outdata is full append it to output_data_file
        i_outdata != n_outdata && continue
        CSV.write(output_data_file, outdata; append=true, delim=delim_outdata)
        nr        += n_outdata
        i_outdata  = 0  # Reset the row number
    end
    if i_outdata != 0
        CSV.write(output_data_file, view(outdata, 1:i_outdata, :); append=true, delim=delim_outdata)
        nr += i_outdata
    end

    # Column-level checks
    datacols_match_schemacols!(issues_in, tableschema, Set(csvrows.names))  # By construction this issue doesn't exist for outdata

    # Format result
    for (colname, colissues) in issues_out[:columnissues]
        colissues[:n_invalid] = 0  # Invalid values have been set to missing in the output data (which are then reported as missing rather than invalid)
    end
    issues_in  = construct_issues_table(issues_in,  tableschema, nr)
    issues_out = construct_issues_table(issues_out, tableschema, nr)
    CSV.write(input_issues_file,  issues_in;  delim=delim_iniss)
    CSV.write(output_issues_file, issues_out; delim=delim_outiss)
end

end
