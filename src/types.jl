struct ColumnSchema
    name::Symbol
    description::String
    eltype::DataType       # The type of each value in the column
    is_categorical::Bool   # Do the values represent categories (nominal or ordinal)?
    cats_ordered::Bool     # If is_categorical, are the categories ordered?
    is_required::Bool      # Is non-missing data required?
    is_unique::Bool        # Is each value in the column unique?
    valid_values::Union{DataType, <:Range}  # Set of permissible values; either the full range of the data type or a user-supplied restricted range.
end


struct TableSchema
    name::Symbol
    description::String
    columns::Vector{Column}
    primary_key::Vector{Column}
    intra_row_constraints::Vector{Function}  # Constraints between columns within a row (e.g., marriage date > birth date)
    inter_row_constraints::Vector{Function}  # Constraints between rows (e.g., a person can have only 1 birth date)
end


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
