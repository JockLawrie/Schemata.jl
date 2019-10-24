module types

export ColumnSchema, TableSchema, Schema

using Dates
using Parsers

import Base.parse  # For extending Base.parse to Base.parse(s::ColumnSchema, val)

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
    parser::Function      # outputvalue = parser(inputvalue)

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
    parser      = constructparser(nothing, nothing, nothing, datatype)
    ColumnSchema(name, description, datatype, iscategorical, isrequired, isunique, validvalues, valueorder, parser)
end

function ColumnSchema(d::Dict)
    name          = d["name"]
    description   = d["description"]
    datatype      = d["datatype"] isa DataType ? d["datatype"] : eval(Meta.parse(d["datatype"]))
    iscategorical = d["iscategorical"]
    isrequired    = d["isrequired"]
    isunique      = d["isunique"]
    if haskey(d, "parser")
        func   = d["parser"]["function"] isa String ? eval(Meta.parse(d["parser"]["function"])) : d["parser"]["function"]
        args   = haskey(d["parser"], "args") ? d["parser"]["args"] : nothing
        kwargs = haskey(d["parser"], "kwargs") ? Dict{Symbol, Any}(Symbol(k) => v for (k,v) in d["parser"]["kwargs"]) : nothing
        parser = constructparser(func, args, kwargs, datatype)
    else     
        parser = constructparser(nothing, nothing, nothing, datatype)
    end
    valueorder  = parse_validvalues(parser, datatype, d["validvalues"])
    validvalues = valueorder isa Vector ? Set(valueorder) : valueorder
    valueorder  = iscategorical ? valueorder : nothing
    ColumnSchema(name, description, datatype, iscategorical, isrequired, isunique, validvalues, valueorder, parser)
end

function constructparser(func, args, kwargs, returntype)
    # Special cases
    if ((func == Date) || (isnothing(func) && returntype == Date)) && !isnothing(args) && length(args) == 1
        df = DateFormat(args[1])
        return (x) -> try Date(x, df) catch e missing end
    end

    # General cases
    if func isa DataType || isnothing(func)
        opts = isnothing(kwargs) ? Parsers.Options() : Parsers.Options(kwargs...)
        function closure(val)
            len = val isa IO ? 0 : sizeof(val)  # Use default pos=1
            x, code, vpos, vlen, tlen = Parsers.xparse(returntype, val isa AbstractString ? codeunits(val) : val, 1, len, opts)
            Parsers.ok(code) ? x : missing
        end
        return closure
    end
    !(func isa Function) && error("Parser function does have type Function.")
    isnothing(args)  && isnothing(kwargs)  && return (x) -> try func(x)                     catch e missing end
    !isnothing(args) && isnothing(kwargs)  && return (x) -> try func(x, args...)            catch e missing end
    isnothing(args)  && !isnothing(kwargs) && return (x) -> try func(x; kwargs...)          catch e missing end
    !isnothing(args) && !isnothing(kwargs) && return (x) -> try func(x, args...; kwargs...) catch e missing end
    error("Invalid specification of the parser.")
end

################################################################################
struct TableSchema
    name::Symbol
    description::String
    colname2colschema::Dict{Symbol, ColumnSchema}
    columnorder::Vector{Symbol}  # Determines the order of the columns
    primarykey::Vector{Symbol}   # Vector of column names
    intrarow_constraints::Vector{Tuple{String, Function}}  # (msg, testfunc). Constraints between columns within a row (e.g., marriage date > birth date)

    function TableSchema(name, description, colname2colschema, columnorder, primarykey, intrarow_constraints=Function[])
        for colname in primarykey
            !haskey(colname2colschema, colname) && error("Table: $(name). Primary key has a non-existent column ($(colname)).")
            colschema = colname2colschema[colname]
            !colschema.isrequired    && error("Table: $(name). Primary key has a column ($(colname)) that allows missing data.")
        end
        if length(primarykey) == 1 && colname2colschema[primarykey[1]].isunique == false
            error("Table: $(name). Primary key must have isunique == true.")
        end
        new(name, description, colname2colschema, columnorder, primarykey, intrarow_constraints)
    end
end

function TableSchema(name, description, colschemata::Vector{ColumnSchema}, primarykey, intrarow_constraints=Function[])
    columnorder       = [colschema.name for colschema in colschemata]
    colname2colschema = Dict(colschema.name => colschema for colschema in colschemata)
    TableSchema(name, description, colname2colschema, columnorder, primarykey, intrarow_constraints)
end

function TableSchema(d::Dict)
    name        = Symbol(d["name"])
    description = d["description"]
    pk          = d["primarykey"]  # String or Vector{String}
    primarykey  = typeof(pk) == String ? [Symbol(pk)] : [Symbol(colname) for colname in pk]
    columns     = d["columns"]
    columnorder = fill(Symbol("x"), size(columns, 1))
    colname2colschema = Dict{Symbol, ColumnSchema}()
    i = 0
    for colname2schema in columns
        for (colname, colschema) in colname2schema
            i += 1
            columnorder[i]    = Symbol(colname)
            colschema["name"] = columnorder[i]
            colname2colschema[columnorder[i]] = ColumnSchema(colschema)
        end
    end
    intrarow_constraints = construct_intrarow_constraints(d)
    TableSchema(name, description, colname2colschema, columnorder, primarykey, intrarow_constraints)
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
