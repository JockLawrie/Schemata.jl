"""
Returns: DataFrame containing ways in which the table does not comply with the schema.

Example result:

  :entity   :id        :issue
   col      patientid  Incorrect data type (String)
   col      patientid  Missing data not allowed
   col      patientid  Values are not unique
   col      gender     Invalid values ('d')
   table    mytable    Primary key not unique
"""
function diagnose(tbl::DataFrame, tbl_schema::TableSchema)
    issues = Dict{Tuple{String, String}, Set{String}}()  # (entity, id) => Set(issue1, issue2, ...)
    table_level_issues!(issues, tbl, tbl_schema)
    column_level_issues!(issues, tbl, tbl_schema.columns, String(tbl_schema.name))
    issues_to_dataframe(issues)
end


"Append table-level issues into issues."
function table_level_issues!(issues, tbl::DataFrame, tbl_schema::TableSchema)
    # Ensure the set of columns in the data matches that in the schema
    tblname         = String(tbl_schema.name)
    colnames_data   = Set(names(tbl))
    colnames_schema = Set([cs.name for cs in tbl_schema.columns])
    cols = setdiff(colnames_data, colnames_schema)
    if length(cols) > 0
        insert_issue!(issues, ("table",tblname), "Data has columns that the schema doesn't have ($(cols)).")
    end
    cols = setdiff(colnames_schema, colnames_data)
    if length(cols) > 0
        insert_issue!(issues, ("table",tblname), "Schema has columns that the data doesn't have ($(cols)).")
    end

    # Ensure that the primary key is unique
    if isempty(setdiff(Set(tbl_schema.primary_key), colnames_data))  # Primary key cols exist in the data
        pk = unique!(tbl[:, tbl_schema.primary_key])
        if size(pk, 1) != size(tbl, 1)
            insert_issue!(issues, ("table",tblname), "Primary key not unique.")
        end
    end
end


"Append table-level issues into issues."
function column_level_issues!(issues, tbl::DataFrame, columns::Vector{ColumnSchema}, tblname::String)
    columns = Dict(col.name => col for col in columns)  # colname => col_schema
    for colname in names(tbl)
        # Collect basic column info
        !haskey(columns, colname) && continue  # This problem is detected at the table level
        schema = columns[colname]
        data   = tbl[colname]
        cm     = countmap(data)
        validvals = schema.valid_values

        # Ensure correct eltype
        if eltype(data) != schema.eltyp
            insert_issue!(issues, ("column", "$tblname.$colname"), "Data has eltype $(eltype(data)), schema requires $(schema.eltyp).")
        end

        # Ensure categorical
        if schema.is_categorical && !(typeof(data) <: DataArrays.PooledDataArray)
            insert_issue!(issues, ("column", "$tblname.$colname"), "Data is not categorical.")
        end

        # Ensure no missing data
        if schema.is_required && haskey(cm, NA)
            insert_issue!(issues, ("column", "$tblname.$colname"), "Missing data not allowed.")
        end

        # Ensure unique data
        if schema.is_unique && length(cm) < size(data, 1)
            insert_issue!(issues, ("column", "$tblname.$colname"), "Values are not unique.")
        end

        # Ensure valid values
        eltype(data) != schema.eltyp && continue  # Only do this check if the data type is valid
        tp = typeof(validvals)
        invalid_values = Set{schema.eltyp}()
        if tp <: Vector || tp <: Range  # eltype(valid_values) has implicitly been checked via the eltype check
            for (val, n) in cm
                isna(val) && continue
                if !in(val, validvals)
                    push!(invalid_values, val)
                end
            end
            if !isempty(invalid_values)
                invalid_values = [x for x in invalid_values]  # Convert Set to Vector
                insert_issue!(issues, ("column", "$tblname.$colname"), "Invalid values: $(invalid_values)")
            end
        end
    end
end


function diagnose(tbl::DataFrame, schema::Schema, tblname::Symbol)
    tbl_schema = schema.tables[1]
    ts_found   = false
    for ts in schema.tables
        if ts.name == tblname
            tbl_schema = ts
            ts_found = true
            break
        end
    end
    !ts_found && error("Table $tblname is not part of the schema.")
    diagnose(tbl, tbl_schema)
end


function diagnose(tbl::DataFrame, schema::Schema)
    length(schema.tables) > 1 && error("Schema has more than 1 TableSchema, please specify which one to compare data to.")
    diagnose(tbl, schema.tables[1])
end



"Init issues[k] if it doesn't already exist, then push msg to issues[k]."
function insert_issue!(issues::Dict{Tuple{String, String}, Set{String}}, k::Tuple{String, String}, msg::String)
    if !haskey(issues, k)
        issues[k] = Set{String}()
    end
    push!(issues[k], msg)
end


function issues_to_dataframe(issues)
    n = 0
    for (ety_id, issue_set) in issues
        n += length(issue_set)
    end
    result = DataFrame(entity = DataArray(String, n), id = DataArray(String, n), issue = DataArray(String, n))
    i = 0
    for (ety_id, issue_set) in issues
        for iss in issue_set
            i += 1
            result[i, :entity] = ety_id[1]
            result[i, :id]     = ety_id[2]
            result[i, :issue]  = iss
        end
    end
    sort!(result, cols=(:entity, :id, :issue))
end
