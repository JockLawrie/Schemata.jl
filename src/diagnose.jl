"""
Returns: Vector of NamedTuples, each of which is a way in which the table does not comply with the schema.

Example result:

  :entity   :id        :issue
   col      patientid  Incorrect data type (String)
   col      patientid  Missing data not allowed
   col      patientid  Values are not unique
   col      gender     Invalid values ('d')
   table    mytable    Primary key not unique
"""
function diagnose(data::Dict{Symbol, T}, schema::Schema) where {T}
    issues = NamedTuple{(:entity, :id, :issue),Tuple{String,String,String}}[]

    # Ensure that the set of tables in the data matches that in the schema
    tblnames_data   = Set(keys(data))
    tblnames_schema = Set(keys(schema.tables))
    tbls = setdiff(tblnames_data, tblnames_schema)
    length(tbls) > 0 && push!(issues, (entity="dataset", id="", issue="Dataset has tables that the schema doesn't have ($(tbls))."))
    tbls = setdiff(tblnames_schema, tblnames_data)
    length(tbls) > 0 && push!(issues, (entity="dataset", id="", issue="Dataset is missing some tables that the Schema has ($(tbls))."))

    # Table and column level diagnoses
    for (tblname, tblschema) in schema.tables
        !haskey(data, tblname) && continue
        diagnose_table!(issues, data[tblname], tblschema)
    end
    issues
end


function diagnose(tbl, tblschema::TableSchema)
    data   = Dict(tblschema.name => tbl)
    schema = Schema(:xxx, "", Dict(tblschema.name => tblschema))
    diagnose(data, schema)
end


"Modified: issues"
function diagnose_table!(issues, tbl, tblschema::TableSchema)
    # Ensure the set of columns in the data matches that in the schema
    tblname         = String(tblschema.name)
    colnames_data   = Set(names(tbl))
    colnames_schema = Set(tblschema.col_order)
    cols = setdiff(colnames_data, colnames_schema)
    length(cols) > 0 && push!(issues, (entity="table", id=tblname, issue="Data has columns that the schema doesn't have ($(cols))."))
    cols = setdiff(colnames_schema, colnames_data)
    length(cols) > 0 && push!(issues, (entity="table", id=tblname, issue="Data is missing some columns that the Schema has ($(cols))."))

    # Ensure that the primary key is unique
    if isempty(setdiff(Set(tblschema.primary_key), colnames_data))  # Primary key cols exist in the data
        pk = unique(tbl[tblschema.primary_key])
        size(pk, 1) != size(tbl, 1) && push!(issues, (entity="table", id=tblname, issue="Primary key not unique."))
    end

    # Column-level issues
    columns = tblschema.columns
    for colname in names(tbl)
        !haskey(columns, colname) && continue  # This problem is detected at the table level
        diagnose_column!(issues, tbl, columns[colname], tblname)
    end

    # Ensure that the intra-row constraints are satisfied
    for (msg, f) in tblschema.intrarow_constraints
        n_badrows = 0
        for r in eachrow(tbl)
            result = @eval $f($r)          # Hack to avoid world age problems. Should use macros instead.
            ismissing(result) && continue  # Only an issue for required values, which is picked up at the column level
            result && continue             # constraint returns true
            n_badrows += 1
        end
        n_badrows == 0 && continue
        if n_badrows == 1
            push!(issues, (entity="table", id=tblname, issue="1 row does not satisfy: $(msg)"))
        else
            push!(issues, (entity="table", id=tblname, issue="$(n_badrows) rows do not satisfy: $(msg)"))
        end
    end
end


"Append table-level issues into issues."
function diagnose_column!(issues, tbl, colschema::ColumnSchema, tblname::String)
    # Collect basic column info
    colname   = colschema.name
    coldata   = tbl[colname]
    vals      = Set{Any}(coldata)  # Type qualifier {Any} allows missing to be a member of the set
    validvals = colschema.valid_values

    # Ensure correct eltype
    data_eltyp_isvalid = true
    schema_eltyp = eltype(colschema)
    if colschema.is_categorical
        data_eltyp = eltype(levels(coldata))
    else
        data_eltyp = Missings.T(eltype(coldata))
    end
    if data_eltyp != schema_eltyp
        data_eltyp_isvalid = false
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Data has eltype $(data_eltyp), schema requires $(schema_eltyp)."))
    end

    # Ensure categorical
    if colschema.is_categorical && !(coldata isa CategoricalVector)
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Data is not categorical."))
    end

    # Ensure no missing data
    if colschema.is_required && in(missing, vals)
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Missing data not allowed."))
    end

    # Ensure unique data
    if colschema.is_unique && length(vals) < size(coldata, 1)
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Values are not unique."))
    end

    # Ensure valid values
    !data_eltyp_isvalid && return  # Only do this check if the data type is valid
    tp = typeof(validvals)
    invalid_values = Set{schema_eltyp}()
    if !(typeof(validvals) <: Dict) && (tp <: Dict || tp <: Vector || tp <: AbstractRange)  # eltype(valid_values) has implicitly been checked via the eltype check
        if typeof(coldata) <: CategoricalArray
            lvls = levels(coldata)
            for val in vals
                ismissing(val) && return
                v = lvls[val.level]
                !value_is_valid(v, validvals) && push!(invalid_values, v)
            end
        else
            for val in vals
                ismissing(val) && return
                !value_is_valid(val, validvals) && push!(invalid_values, val)
            end
        end
    end
    if !isempty(invalid_values)
        invalid_values = [x for x in invalid_values]  # Convert Set to Vector
        sort!(invalid_values)
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Invalid values: $(invalid_values)"))
    end
end
