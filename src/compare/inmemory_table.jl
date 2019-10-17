"""
Compare an in-memory table to a table schema.
"""
module inmemory_table

using DataFrames
using CategoricalArrays
using Tables

using ..handle_validvalues
using ..types
using ..common

function compare(tableschema::TableSchema, indata, sorted_by_primarykey::Bool)
    # Init
    tablename     = tableschema.name
    outdata       = init_outdata(tableschema, size(indata, 1))
    issues_in     = init_issues(tableschema)  # Issues for indata
    issues_out    = init_issues(tableschema)  # Issues for outdata
    pk_colnames   = tableschema.primarykey
    pk_ncols      = length(pk_colnames)
    primarykey    = missings(String, pk_ncols)  # Stringified primary key 
    pkvalues_in   = Set{String}()  # Values of the primary key
    pkvalues_out  = Set{String}()
    pk_issues_in  = pk_ncols == 1 ? issues_in[:columnissues][pk_colnames[1]]  : Dict{Symbol, Int}()
    pk_issues_out = pk_ncols == 1 ? issues_out[:columnissues][pk_colnames[1]] : Dict{Symbol, Int}()
    i_outdata     = 0
    nconstraints  = length(tableschema.intrarow_constraints)
    colname2colschema = tableschema.colname2colschema
    uniquevalues_in   = Dict(colname => Set{nonmissingtype(eltype(getproperty(indata, colname)))}() for (colname, colschema) in colname2colschema if colschema.isunique==true)
    uniquevalues_out  = Dict(colname => Set{colschema.datatype}() for (colname, colschema) in colname2colschema if colschema.isunique==true)

    # Row-level checks
    for inputrow in Tables.rows(indata)
        # Parse inputrow into outputrow according to ColumnSchema
        i_outdata += 1
        outputrow  = outdata[i_outdata, :]
        parserow!(outputrow, inputrow, colname2colschema)

        # Assess input row
        if pk_ncols == 1
            pk_n_missing   = pk_issues_in[:n_missing]    # Number of earlier rows (excluding the curent row) with missing primary key
            pk_n_notunique = pk_issues_in[:n_notunique]  # Number of earlier rows (excluding the curent row) with duplicated primary key
        end
        assess_row!(issues_in[:columnissues], inputrow, colname2colschema, uniquevalues_in)
        nconstraints > 0 && test_intrarow_constraints!(issues_in[:intrarow_constraints], tableschema, inputrow)
        if pk_ncols == 1
            pk_incomplete, pk_duplicated = assess_singlecolumn_primarykey!(issues_in, pk_issues_in, pk_n_missing, pk_n_notunique)
        else
            pk_incomplete, pk_duplicated = assess_multicolumn_primarykey!(issues_in, primarykey, pk_colnames, pkvalues_in, sorted_by_primarykey, inputrow)
        end

        # Assess output row
        if pk_ncols == 1
            pk_n_missing   = pk_issues_out[:n_missing]    # Number of earlier rows (excluding the curent row) with missing primary key
            pk_n_notunique = pk_issues_out[:n_notunique]  # Number of earlier rows (excluding the curent row) with duplicated primary key
        end
        assess_row_set_invalid_to_missing!(issues_out[:columnissues], outputrow, colname2colschema, uniquevalues_out)
        nconstraints > 0 && test_intrarow_constraints!(issues_out[:intrarow_constraints], tableschema, outputrow)
        if pk_ncols == 1
            pk_incomplete, pk_duplicated = assess_singlecolumn_primarykey!(issues_out, pk_issues_out, pk_n_missing, pk_n_notunique)
        else
            pk_incomplete, pk_duplicated = assess_multicolumn_primarykey!(issues_out, primarykey, pk_colnames, pkvalues_out, sorted_by_primarykey, outputrow)
        end
    end

    # Column-level checks
    for (colname, colschema) in colname2colschema
        !colschema.iscategorical && continue
        categorical!(outdata, colname)
    end
    datacols_match_schemacols!(issues_in, tableschema, Set(propertynames(indata)))  # By construction this issue doesn't exist for outdata
    compare_datatypes!(issues_in,  indata,  colname2colschema)
    compare_datatypes!(issues_out, outdata, colname2colschema)

    # Format result
    issues_in  = construct_issues_table(issues_in,  tableschema, i_outdata)
    issues_out = construct_issues_table(issues_out, tableschema, i_outdata)
    outdata, issues_in, issues_out
end

"""
Modified: issues.

Checks whether each column and its schema are both categorical or both not categorical, and whether they have the same data types.
"""
function compare_datatypes!(issues, table, colname2colschema)
    for (colname, colschema) in colname2colschema
        coldata    = getproperty(table, colname)
        data_eltyp = colschema.iscategorical ? eltype(levels(coldata)) : nonmissingtype(eltype(coldata))
        if data_eltyp != colschema.datatype  # Check data type matches that specified in the ColumnSchema
            issues[:columnissues][colname][:different_datatypes] = 1
        end
        if colschema.iscategorical && !(coldata isa CategoricalVector)  # Ensure categorical values
            issues[:columnissues][colname][:data_not_categorical] = 1
        end
        if !colschema.iscategorical && coldata isa CategoricalVector    # Ensure non-categorical values
            issues[:columnissues][colname][:data_is_categorical] = 1
        end
    end
end

"Sets invalid values to missing."
function assess_row_set_invalid_to_missing!(columnissues, outputrow, colname2colschema, uniquevalues)
    for (colname, colschema) in colname2colschema
        val = outputrow[colname]
        if ismissing(val)
            !colschema.isrequired && continue  # Checks are done before the function call to avoid unnecessary dict lookups
            assess_missing_value!(columnissues[colname])
        elseif value_is_valid(val, colschema.validvalues)
            !colschema.isunique && continue
            assess_nonmissing_value!(columnissues[colname], val, uniquevalues[colname])
        else
            outputrow[colname] = missing
            !colschema.isrequired && continue
            assess_missing_value!(columnissues[colname])
        end
    end
end

end
