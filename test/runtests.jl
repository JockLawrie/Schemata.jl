using Base.Test


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

cs = ColumnSchema(:customer_id, "Customer ID", Int, false, false, true, true, Int)


@test 1 == 1
