module schematypes

export ColumnSchema, TableSchema, Schema

using Dates

using ..CustomParsers
using ..handle_validvalues


mutable struct ColumnSchema
    name::Symbol
    description::String
    datatype::DataType    # The non-missing type of each value in the column
    iscategorical::Bool   # Specifies whether the values represent categories. If so, order is specified by valueorder.
    isrequired::Bool      # Is non-missing data required?
    isunique::Bool        # Is each value in the column unique?
    validvalues::Union{DataType, <:AbstractRange, <:Set}             # Either the full range of the data type or a user-supplied restriction.
    valueorder::Union{DataType, <:AbstractRange, <:Vector, Nothing}  # If iscategorical, valueorder specifies the ordering of categories. Else nothing.
    parser::CustomParser  # Specifies how values are parsed to non-base types

    function ColumnSchema(name, description, datatype, iscategorical, isrequired, isunique, validvalues, valueorder, parser)
        # Ensure eltyp and validvalues are consistent with each other
        tp_validvals = get_datatype(validvalues)
        datatype != tp_validvals && error("Column :$(name). Type of valid values ($(tp_validvals)) does not match that of eltype ($(datatype)).")
        new(Symbol(name), description, datatype, iscategorical, isrequired, isunique, validvalues, valueorder, parser)
    end
end


function ColumnSchema(name, description, datatype, iscategorical, isrequired, isunique, validvalues)
    valueorder  = iscategorical ? validvalues : nothing
    validvalues = validvalues isa Vector ? Set(validvalues) : validvalues
    parser      = CustomParser(datatype)
    ColumnSchema(name, description, datatype, iscategorical, isrequired, isunique, validvalues, valueorder, parser)
end


function ColumnSchema(d::Dict)
    datatype = d["datatype"] isa DataType ? d["datatype"] : eval(Meta.parse(d["datatype"]))
    if haskey(d, "parser") && !haskey(d["parser"], "returntype")
        d["parser"]["returntype"] = datatype
    end
    name          = d["name"]
    description   = d["description"]
    parser        = haskey(d, "parser") ? CustomParser(d["parser"]) : CustomParser(datatype)
    iscategorical = d["iscategorical"]
    isrequired    = d["isrequired"]
    isunique      = d["isunique"]
    valueorder    = parse_validvalues(parser, d["validvalues"])
    validvalues   = valueorder isa Vector ? Set(valueorder) : valueorder
    valueorder    = iscategorical ? valueorder : nothing
    ColumnSchema(name, description, datatype, iscategorical, isrequired, isunique, validvalues, valueorder, parser)
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

end
