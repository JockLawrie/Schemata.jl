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
function diagnose{T}(data::Dict{Symbol, T}, schema::Schema)
    issues = Dict{Tuple{String, String}, Set{String}}()  # (entity, id) => Set(issue1, issue2, ...)

    # Ensure that the set of tables in the data matches that in the schema
    tblnames_data   = Set(keys(data))
    tblnames_schema = Set(keys(schema.tables))
    tbls = setdiff(tblnames_data, tblnames_schema)
    if length(tbls) > 0
        insert_issue!(issues, ("dataset",""), "Dataset has tables that the schema doesn't have ($(tbls)).")
    end
    tbls = setdiff(tblnames_schema, tblnames_data)
    if length(tbls) > 0
        insert_issue!(issues, ("dataset",""), "Dataset is missing some tables that the Schema has ($(tbls)).")
    end

    # Table and column level diagnoses
    for (tblname, tblschema) in schema.tables
        !haskey(data, tblname) && continue
        diagnose_table!(issues, data[tblname], tblschema)
    end
    format_issues(issues)
end


function diagnose(tbl, tbl_schema::TableSchema)
    data   = Dict(tbl_schema.name => tbl)
    schema = Schema(:xxx, "", [tbl_schema])
    diagnose(data, schema)
end


"Modified: issues"
function diagnose_table!(issues, tbl, tbl_schema::TableSchema)
    table_level_issues!(issues, tbl, tbl_schema)
    column_level_issues!(issues, tbl, tbl_schema.columns, String(tbl_schema.name))
end


"Append table-level issues into issues."
function table_level_issues!(issues, tbl, tbl_schema::TableSchema)
    # Ensure the set of columns in the data matches that in the schema
    tblname         = String(tbl_schema.name)
    colnames_data   = Set(names(tbl))
    colnames_schema = Set(tbl_schema.col_order)
    cols = setdiff(colnames_data, colnames_schema)
    if length(cols) > 0
        insert_issue!(issues, ("table",tblname), "Data has columns that the schema doesn't have ($(cols)).")
    end
    cols = setdiff(colnames_schema, colnames_data)
    if length(cols) > 0
        insert_issue!(issues, ("table",tblname), "Data is missing some columns that the Schema has ($(cols)).")
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
function column_level_issues!(issues, tbl, columns::Dict{Symbol, ColumnSchema}, tblname::String)
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


"Init issues[k] if it doesn't already exist, then push msg to issues[k]."
function insert_issue!(issues::Dict{Tuple{String, String}, Set{String}}, k::Tuple{String, String}, msg::String)
    if !haskey(issues, k)
        issues[k] = Set{String}()
    end
    push!(issues[k], msg)
end


function format_issues(issues)
    # Count number of issues
    nissues = 0
    for (ety_id, issue_set) in issues
        nissues += length(issue_set)
    end

    # Construct result
    result = issues
    if isdefined(Main, :DataFrame)
        result = issues_to_dataframe(issues, nissues)
    else
        result = issues_to_vector(issues, nissues)
    end
    result
end


function issues_to_vector(issues, nissues::Int)
    result = fill(("","",""), nissues)
    i = 0
    for (ety_id, issue_set) in issues
        for iss in issue_set
            i += 1
            result[i] = (ety_id[1], ety_id[2], iss)
        end
    end
    sort!(result)
end


function issues_to_dataframe(issues, nissues::Int)
    m = Main
    result = m.DataFrame(entity = m.DataArray(String, nissues), id = m.DataArray(String, nissues), issue = m.DataArray(String, nissues))
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
