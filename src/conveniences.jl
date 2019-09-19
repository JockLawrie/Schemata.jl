module conveniences

export get_columnschema, insertcolumn! 

using ..schematypes


get_columnschema(tableschema::TableSchema, colname::Symbol) = tableschema.columns[colname]

"Insert a column into the table schema at position n."
function insertcolumn!(tblschema::TableSchema, colschema::ColumnSchema, n::Int=-1)
    # Collect basic info
    colname     = colschema.name
    columnorder = tblschema.columnorder
    n = n < 0 ? size(columnorder, 1) + 1 : n  # Default: insert column at the end

    # Remove column if it already exists
    if haskey(tblschema.columns, colname)
        n = findfirst(columnorder, colname)   # New column will be inserted at the same position as the old column
        splice!(columnorder, n)
    end

    # Insert column
    tblschema.columns[colname] = colschema
    if n == size(columnorder, 1) + 1   # Insert column at the end
        push!(columnorder, colname)
    elseif n == 1                      # Insert column at the beginning
        unshift!(columnorder, colname)
    else
        splice!(columnorder, n:(n-1), colname)
    end
end


end
