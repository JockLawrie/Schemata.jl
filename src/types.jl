mutable struct ColumnSchema
    name::Symbol
    description::String
    eltyp::DataType      # The type of each value in the column. Dict contains "type"=>some_type, plus options.
    iscategorical::Bool  # Specifies whether the values represent categories. If so, order is specified by valueorder.
    isrequired::Bool     # Is non-missing data required?
    isunique::Bool       # Is each value in the column unique?
    validvalues::Union{DataType, <:AbstractRange, <:Set}             # Either the full range of the data type or a user-supplied restriction.
    valueorder::Union{DataType, <:AbstractRange, <:Vector, Nothing}  # If iscategorical is true, valueorder specifies the ordering of the categories. Else nothing.

    function ColumnSchema(name, description, eltyp, iscategorical, isrequired, isunique, validvalues, valueorder)
        # Ensure eltyp is either a DataType or a Dict containing "type" => some_type
        #=
        tp_eltyp = typeof(eltyp)
        if tp_eltyp <: Dict
            !haskey(eltyp, "type") && error("Eltype is a Dict without pair type=>some_type.")
            typeof(eltyp["type"]) != DataType && error("Eltype is a Dict with pair type=>val, but val is not a DataType.")

            # Ensure keys "args" and "kwargs" have Vector values
            if haskey(eltyp, "args") && !(typeof(eltyp["args"]) <: Vector)
                eltyp["args"] = [eltyp["args"]]  # wrap in vector
            end
            if !haskey(eltyp, "args")
                eltyp["args"] = []
            end
            if haskey(eltyp, "kwargs") && !(typeof(eltyp["kwargs"]) <: Vector)
                eltyp["kwargs"] = [eltyp["kwargs"]]  # wrap in vector
            end
            if !haskey(eltyp, "kwargs")
                eltyp["kwargs"] = []
            end
        end
        =#

        # Ensure eltyp and validvalues are consistent with each other
        tp_validvals = get_datatype(validvalues)
        eltyp != tp_validvals && error("Column :$(name). Type of valid values ($(tp_validvals)) does not match that of eltype ($(eltyp)).")
        new(Symbol(name), description, eltyp, iscategorical, isrequired, isunique, validvalues, valueorder)
    end
end


function ColumnSchema(name, description, eltyp, iscategorical, isrequired, isunique, validvalues)
    valueorder  = iscategorical ? validvalues : nothing
    validvalues = validvalues isa Vector ? Set(validvalues) : validvalues
    ColumnSchema(name, description, eltyp, iscategorical, isrequired, isunique, validvalues, valueorder)
end


function ColumnSchema(d::Dict)
    name          = d["name"]
    descr         = d["description"]
    eltyp         = d["datatype"] isa DataType ? d["datatype"] : eval(Meta.parse(d["datatype"]))
    iscategorical = d["categorical"]
    isrequired    = d["required"]
    isunique      = d["unique"]
    valueorder    = determine_validvalues(d["validvalues"], eltyp)
    validvalues   = valueorder isa Vector ? Set(valueorder) : valueorder
    valueorder    = iscategorical ? valueorder : nothing
    ColumnSchema(name, descr, eltyp, iscategorical, isrequired, isunique, validvalues, valueorder)
end


################################################################################
struct TableSchema
    name::Symbol
    description::String
    columns::Dict{Symbol, ColumnSchema}  # colname => col_schema
    columnorder::Vector{Symbol}          # Determines the order of the columns
    primarykey::Vector{Symbol}           # Vector of column names
    intrarow_constraints::Vector{Tuple{String, Function}}  # (msg, testfunc). Constraints between columns within a row (e.g., marriage date > birth date)

    function TableSchema(name, description, columns, columnorder, primarykey, intrarow_constraints=Function[])
        for colname in primarykey
            !haskey(columns, colname) && error("Table :$(name). Primary key has a non-existent column ($(colname)).")
            colschema = columns[colname]
            !colschema.isrequired    && error("Table :$(name). Primary key has a column ($(colname)) that allows missing data.")
        end
        new(name, description, columns, columnorder, primarykey, intrarow_constraints)
    end
end


function TableSchema(name, description, columns::Vector{ColumnSchema}, primarykey, intrarow_constraints=Function[])
    columnorder = [col.name for col in columns]
    columns   = Dict(col.name => col for col in columns)
    TableSchema(name, description, columns, columnorder, primarykey, intrarow_constraints)
end


function TableSchema(d::Dict)
    name        = Symbol(d["name"])
    description = d["description"]
    pk          = d["primarykey"]  # String or Vector{String}
    primarykey = typeof(pk) == String ? [Symbol(pk)] : [Symbol(colname) for colname in pk]
    cols        = d["columns"]
    columns     = Dict{Symbol, ColumnSchema}()
    columnorder   = fill(Symbol("x"), size(cols, 1))
    i = 0
    for colname2schema in cols
        for (colname, colschema) in colname2schema
            i += 1
            columnorder[i]          = Symbol(colname)
            colschema["name"]       = columnorder[i]
            columns[columnorder[i]] = ColumnSchema(colschema)
        end
    end
    intrarow_constraints = construct_intrarow_constraints(d)
    TableSchema(name, description, columns, columnorder, primarykey, intrarow_constraints)
end


function construct_intrarow_constraints(d::Dict)
    !haskey(d, "intrarow_constraints") && return Tuple{String, Function}[]
    d = d["intrarow_constraints"]
    n = length(d)
    result = Vector{Tuple{String, Function}}(undef, n)
    i = 0
    for (msg, func) in d
      s  = "(r) -> $(func)"
      f  = eval(Meta.parse(s))
      i += 1
      result[i] = (msg, f)
    end
    result
end


################################################################################
struct Schema
    name::Symbol
    description::String
    tables::Dict{Symbol, TableSchema}  # table_name => table_schema

    function Schema(name, description, tables)
        new(name, description, tables)
    end
end

function Schema(d::Dict)
    name = Symbol(d["name"])
    description = d["description"]
    tables = Dict{Symbol, TableSchema}()
    for (tblname, tblschema) in d["tables"]
        tblschema["name"]       = tblname
        tables[Symbol(tblname)] = TableSchema(tblschema)
    end
    Schema(name, description, tables)
end
