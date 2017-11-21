struct ColumnSchema
    name::Symbol
    description::String
    eltyp::DataType        # The type of each value in the column
    is_categorical::Bool   # Do the values represent categories (either nominal or ordinal)?
    cats_ordered::Bool     # If is_categorical, are the categories ordered?
    is_required::Bool      # Is non-missing data required?
    is_unique::Bool        # Is each value in the column unique?
    valid_values::Union{DataType, <:Range, <:Set, <: Vector}  # Either the full range of the data type or a user-supplied restriction.

    function ColumnSchema(name, description, eltyp, is_categorical, cats_ordered, is_required, is_unique, valid_values)
        tp = typeof(valid_values)
        if tp == DataType && valid_values != eltyp
            error("Column :$name. Type of valid_values ($valid_values) does not match eltype ($eltyp).")
        elseif eltype(valid_values) != eltyp && valid_values != eltyp   # Last clause included because eltype(String) == Char
            error("Column :$name. Type of valid_values ($(eltype(valid_values))) does not match eltype ($eltyp).")
        end
        new(name, description, eltyp, is_categorical, cats_ordered, is_required, is_unique, valid_values)
    end
end


struct TableSchema
    name::Symbol
    description::String
    columns::Vector{ColumnSchema}
    primary_key::Vector{Symbol}              # vector of column names
    intra_row_constraints::Vector{Function}  # Constraints between columns within a row (e.g., marriage date > birth date)
    inter_row_constraints::Vector{Function}  # Constraints between rows (e.g., a person can have only 1 birth date)

    function TableSchema(name, description, columns, primary_key, intra_row_constraints, inter_row_constraints)
        colnames = Dict(col.name => col for col in columns)
        for colname in primary_key
            !haskey(colnames, colname) && error("Table :$name. Primary key has a non-existent column ($colname).")
            colschema = colnames[colname]
            !colschema.is_required     && error("Table :$name. Primary key has a column ($colname) that allows missing data.")
        end
        new(name, description, columns, primary_key, intra_row_constraints, inter_row_constraints)
    end
end


struct Schema
    name::Symbol
    tables::Vector{TableSchema}
end


#=
struct Join
    table1::TableSchema
    table2::TableSchema
    columns::Dict{ColumnSchema, ColumnSchema}
end


struct MultiTableSchema
    name::Symbol
    tables::Vector{TableSchema}
    joins::Vector{Join}
    intra_row_constraints::Vector{Function}  # Constraints between columns within a row (e.g., tbl1.marriage_date > tbl2.birth_date)
    inter_row_constraints::Vector{Function}  # Constraints between rows (e.g., a person can have only 1 birth date)
end
=#
