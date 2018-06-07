mutable struct ColumnSchema
    name::Symbol
    description::String
    eltyp::Union{DataType, Dict}  # The type of each value in the column. Dict contains "type"=>some_type, plus options.
    is_categorical::Bool          # Specifies whether the values represent categories. If so, order is specified by valid_values.
    is_required::Bool             # Is non-missing data required?
    is_unique::Bool               # Is each value in the column unique?
    valid_values::Union{DataType, Dict, <:Range, <:Vector}  # Either the full range of the data type or a user-supplied restriction.

    function ColumnSchema(name, description, eltyp, is_categorical, is_required, is_unique, valid_values)
        # Ensure eltyp is either a DataType or a Dict containing "type" => some_type
        tp_eltyp = typeof(eltyp)
        !(tp_eltyp == DataType || tp_eltyp <: Dict) && error("ColumnSchema eltype is neither a DataType nor a Dict.")
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

        # Ensure eltyp and valid_values are consistent with each other
        tp_eltyp     = get_datatype(eltyp)
        tp_validvals = get_datatype(valid_values)
        tp_eltyp    != tp_validvals && error("Column :$name. Type of valid values ($tp_validvals) does not match that of eltype ($tp_eltyp).")
        new(name, description, eltyp, is_categorical, is_required, is_unique, valid_values)
    end
end


function ColumnSchema(dct::Dict)
    name           = dct["name"]
    descr          = dct["description"]
    eltyp          = determine_eltype(dct["datatype"])
    is_categorical = dct["categorical"]
    is_required    = dct["required"]
    is_unique      = dct["unique"]
    validvalues    = determine_validvalues(dct["validvalues"], eltyp)
    ColumnSchema(name, descr, eltyp, is_categorical, is_required, is_unique, validvalues)
end


import Base.eltype
"Get eltyp from existing ColumnSchema."
function eltype(colschema::ColumnSchema)
    get_datatype(colschema.eltyp)
end

get_datatype(vv::DataType) = vv
get_datatype(vv::Dict)     = vv["type"]
get_datatype(vv::Range)    = typeof(vv[1])
get_datatype(vv::Vector)   = eltype(vv)


################################################################################
struct TableSchema
    name::Symbol
    description::String
    columns::Dict{Symbol, ColumnSchema}     # colname => col_schema
    col_order::Vector{Symbol}               # Determines the order of the columns
    primary_key::Vector{Symbol}             # vector of column names
    intrarow_constraints::Vector{Tuple{String, Function}}  # (msg, testfunc). Constraints between columns within a row (e.g., marriage date > birth date)

    function TableSchema(name, description, columns, col_order, primary_key, intrarow_constraints=Function[])
        for colname in primary_key
            !haskey(columns, colname) && error("Table :$name. Primary key has a non-existent column ($colname).")
            colschema = columns[colname]
            !colschema.is_required    && error("Table :$name. Primary key has a column ($colname) that allows missing data.")
        end
        new(name, description, columns, col_order, primary_key, intrarow_constraints)
    end
end


function TableSchema(name, description, columns::Vector{ColumnSchema}, primary_key, intrarow_constraints=Function[])
    col_order = [col.name for col in columns]
    columns   = Dict(col.name => col for col in columns)
    TableSchema(name, description, columns, col_order, primary_key, intrarow_constraints)
end


function TableSchema(dct::Dict)
    name        = Symbol(dct["name"])
    description = dct["description"]
    pk          = dct["primary_key"]  # String or Vector{String}
    primary_key = typeof(pk) == String ? [Symbol(pk)] : [Symbol(colname) for colname in pk]
    cols        = dct["columns"]
    columns     = Dict{Symbol, ColumnSchema}()
    col_order   = fill(Symbol("x"), size(cols, 1))
    i = 0
    for colname2schema in cols
        for (colname, colschema) in colname2schema
            i += 1
            col_order[i]          = Symbol(colname)
            colschema["name"]     = col_order[i]
            columns[col_order[i]] = ColumnSchema(colschema)
        end
    end
    intrarow_constraints = haskey(dct, "intrarow_constraints") ? construct_intrarow_constraints(dct["intrarow_constraints"]) : Tuple{String, Function}[]
    TableSchema(name, description, columns, col_order, primary_key, intrarow_constraints)
end


function construct_intrarow_constraints(dct::Dict)
    n = length(dct)
    result = Vector{Tuple{String, Function}}(n)
    i = 0
    for (msg, func) in dct
      s  = "(r) -> $(func)"
      f  = eval(parse(s))
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

function Schema(dct::Dict)
    name = Symbol(dct["name"])
    description = dct["description"]
    tables = Dict{Symbol, TableSchema}()
    for (tblname, tblschema) in dct["tables"]
        tblschema["name"]       = tblname
        tables[Symbol(tblname)] = TableSchema(tblschema)
    end
    Schema(name, description, tables)
end

#=
struct Join
    table1::Symbol
    table2::Symbol
    columns::Dict{Symbol, Symbol}  # table1.column => table2.column
end
=#

################################################################################
### Methods

"Insert a column into the table schema at position n."
function insert_column!(tblschema::TableSchema, colschema::ColumnSchema, n::Int=-1)
    # Collect basic info
    colname   = colschema.name
    col_order = tblschema.col_order
    n = n < 0 ? size(col_order, 1) + 1 : n  # Default: insert column at the end

    # Remove column if it already exists
    if haskey(tblschema.columns, colname)
        n = findfirst(col_order, colname)  # new column will be inserted at the same position as the old column
        splice!(col_order, n)
    end

    # Insert column
    tblschema.columns[colname] = colschema
    if n == size(col_order, 1) + 1     # Insert column at the end
        push!(col_order, colname)
    elseif n == 1                      # Insert column at the beginning
        unshift!(col_order, colname)
    else
        splice!(col_order, n:(n-1), colname)
    end
end
