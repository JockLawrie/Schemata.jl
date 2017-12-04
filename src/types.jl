mutable struct ColumnSchema
    name::Symbol
    description::String
    eltyp::DataType        # The type of each value in the column
    is_categorical::Bool   # Specifies whether the values represent categories. If so, order is specified by valid_values.
    is_required::Bool      # Is non-missing data required?
    is_unique::Bool        # Is each value in the column unique?
    valid_values::Union{DataType, <:Range, <:Vector}  # Either the full range of the data type or a user-supplied restriction.

    function ColumnSchema(name, description, eltyp, is_categorical, is_required, is_unique, valid_values)
        tp = typeof(valid_values)
        if tp == DataType && valid_values != eltyp
            error("Column :$name. Type of valid_values ($valid_values) does not match eltype ($eltyp).")
        elseif eltype(valid_values) != eltyp && valid_values != eltyp   # Last clause included because eltype(String) == Char
            error("Column :$name. Type of valid_values ($(eltype(valid_values))) does not match eltype ($eltyp).")
        end
        new(name, description, eltyp, is_categorical, is_required, is_unique, valid_values)
    end
end


################################################################################
struct TableSchema
    name::Symbol
    description::String
    columns::Dict{Symbol, ColumnSchema}     # colname => col_schema
    col_order::Vector{Symbol}               # Determines the order of the columns
    primary_key::Vector{Symbol}             # vector of column names
    intrarow_constraints::Vector{Function}  # Constraints between columns within a row (e.g., marriage date > birth date)

    function TableSchema(name, description, columns, col_order, primary_key, intrarow_constraints)
        for colname in primary_key
            !haskey(columns, colname) && error("Table :$name. Primary key has a non-existent column ($colname).")
            colschema = columns[colname]
            !colschema.is_required    && error("Table :$name. Primary key has a column ($colname) that allows missing data.")
        end
        new(name, description, columns, col_order, primary_key, intrarow_constraints)
    end
end


function TableSchema(name, description, columns::Vector{ColumnSchema}, primary_key)
    col_order = [col.name for col in columns]
    columns   = Dict(col.name => col for col in columns)
    TableSchema(name, description, columns, col_order, primary_key, Function[]) 
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
