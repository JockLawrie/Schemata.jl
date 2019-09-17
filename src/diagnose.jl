module diagnosedata

export diagnose

using CategoricalArrays

using ..handle_validvalues
using ..schematypes

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

diagnose(tbl, tblschema::TableSchema) = diagnose_table!(NamedTuple{(:entity, :id, :issue),Tuple{String,String,String}}[], tbl, tblschema)


"Modified: issues"
function diagnose_table!(issues, tbl, tblschema::TableSchema)
    # Ensure the set of columns in the data matches that in the schema
    tblname         = String(tblschema.name)
    colnames_data   = Set(names(tbl))
    colnames_schema = Set(tblschema.columnorder)
    cols = setdiff(colnames_data, colnames_schema)
    length(cols) > 0 && push!(issues, (entity="table", id=tblname, issue="Data has columns that the schema doesn't have ($(cols))."))
    cols = setdiff(colnames_schema, colnames_data)
    length(cols) > 0 && push!(issues, (entity="table", id=tblname, issue="Data is missing some columns that the Schema has ($(cols))."))

    # Ensure that the primary key is unique
    if isempty(setdiff(Set(tblschema.primarykey), colnames_data))  # Primary key cols exist in the data
        pk = unique(tbl[!, tblschema.primarykey])
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
    issues
end


"Append table-level issues into issues."
function diagnose_column!(issues, tbl, colschema::ColumnSchema, tblname::String)
    # Collect basic column info
    colname   = colschema.name
    coldata   = tbl[!, colname]
    vals      = Set{Any}(coldata)  # Type qualifier {Any} allows missing to be a member of the set
    validvals = colschema.validvalues

    # Ensure correct datatype
    colschema_datatype = colschema.datatype
    if colschema.iscategorical
        data_eltyp = eltype(levels(coldata))
    else
        data_eltyp = Core.Compiler.typesubtract(eltype(coldata), Missing)
    end
    if data_eltyp != colschema_datatype
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Data has eltype $(data_eltyp), schema requires $(colschema_datatype)."))
    end

    # Ensure categorical
    if colschema.iscategorical && !(coldata isa CategoricalVector)
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Data is not categorical."))
    end

    # Ensure no missing data
    if colschema.isrequired && in(missing, vals)
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Missing data not allowed."))
    end

    # Ensure unique data
    if colschema.isunique && length(vals) < size(coldata, 1)
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Values are not unique."))
    end

    # Ensure valid values
    data_eltyp != colschema_datatype && return  # Only check values are valid if the values' data type is valid
    validvals isa DataType           && return  # validvals == colschema.datatype and colschema.datatype == data_eltyp 
    invalidvalues = Set{colschema_datatype}()
    if coldata isa CategoricalArray
        lvls = levels(coldata)
        for val in vals
            ismissing(val) && continue
            v = lvls[val.level]
            !value_is_valid(v, validvals) && push!(invalidvalues, v)
            length(invalidvalues) == 5 && break  # Record a maximum of 5 invalid values in the issues table
        end
    else
        for val in vals
            ismissing(val) && continue
            !value_is_valid(val, validvals) && push!(invalidvalues, val)
            length(invalidvalues) == 5 && break  # Record a maximum of 5 invalid values in the issues table
        end
    end
    if !isempty(invalidvalues)
        invalidvalues = [x for x in invalidvalues]  # Convert Set to Vector
        sort!(invalidvalues)
        push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Invalid values: $(invalidvalues)"))
    end
end

end
