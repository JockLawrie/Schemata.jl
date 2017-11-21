struct ColumnSchema
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


struct TableSchema
    name::Symbol
    description::String
    columns::Vector{ColumnSchema}
    primary_key::Vector{Symbol}             # vector of column names
    intrarow_constraints::Vector{Function}  # Constraints between columns within a row (e.g., marriage date > birth date)

    function TableSchema(name, description, columns, primary_key, intrarow_constraints)
        colnames = Dict(col.name => col for col in columns)
        for colname in primary_key
            !haskey(colnames, colname) && error("Table :$name. Primary key has a non-existent column ($colname).")
            colschema = colnames[colname]
            !colschema.is_required     && error("Table :$name. Primary key has a column ($colname) that allows missing data.")
        end
        new(name, description, columns, primary_key, intrarow_constraints)
    end
end
TableSchema(name, description, columns, primary_key) = TableSchema(name, description, columns, primary_key, Function[]) 


struct Schema
    name::Symbol
    tables::Vector{TableSchema}
end


#=
struct Join
    table1::Symbol
    table2::Symbol
    columns::Dict{Symbol, Symbol}  # table1.column => table2.column
end
=#
