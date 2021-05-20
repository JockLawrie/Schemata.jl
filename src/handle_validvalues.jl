module handle_validvalues

export parse_validvalues, get_datatype, value_is_valid

using CategoricalArrays
using DataAPI
using Dates

"Returns: The type of each valid value (after validvalues has been parsed)."
get_datatype(validvalues::DataType) = validvalues
get_datatype(validvalues::Set)      = eltype(validvalues)
get_datatype(validvalues::T) where {T <: AbstractRange} = typeof(validvalues[1])

"Returns: True if value is in validvalues"
value_is_valid(value::T, validvalues::DataType) where {T <: CategoricalValue} = typeof(DataAPI.unwrap(value)) == validvalues
value_is_valid(value::T, validvalues::DataType) where {T <: CategoricalValue{String, <:Integer}} = typeof(DataAPI.unwrap(value)) == validvalues
value_is_valid(value::T, validvalues)           where {T <: CategoricalValue} = value_is_valid(DataAPI.unwrap(value), validvalues)
value_is_valid(value::T, validvalues)           where {T <: CategoricalValue{String, <:Integer}} = value_is_valid(DataAPI.unwrap(value), validvalues)
value_is_valid(value, validvalues::DataType) = typeof(value) == validvalues
value_is_valid(value, validvalues) = typeof(value) == eltype(validvalues) && in(value, validvalues)

"""
Returns: An instance of ColumnSchema.validvalues.

validvalues could be a:
- DataType. E.g., "Int64"
- Range of a Core type. E.g., "1:10"
- Range of a non-Core type, represented with a tuple: "(val1, stepsize, val2)"
"""
function parse_validvalues(parser::Function, returntype::DataType, validvalues::String)
    validvalues[1] == '(' && return parse_noncore_range(parser, returntype, validvalues)  # Range of a non-Core type

    # DataType
    try
        eval(Meta.parse(validvalues)) == returntype && return returntype  # "Int" becomes Int64
    catch e
    end

    # Range of a Core type
    vals = strip.(String.(split(validvalues, ':')))
    val1 = parser(vals[1])
    val2 = parser(vals[2])
    length(vals) == 2 && return val1:val2       # Step-size not supplied
    val3 = parser(vals[3])
    length(vals) == 3 && return val1:val2:val3  # Step-size supplied

    # Else error
    error("Cannot parse validvalues: $(validvalues)")
end


function parse_validvalues(parser::Function, returntype::DataType, validvalues::T) where {T <: Vector}
    eltype(validvalues) == returntype ? validvalues : parser.(validvalues)
end


"""
Parse String to a range of values with non-Core type.

The format is: "(start, stepsize, stop)"

Example: (2017-10-01 09:00+10:00, Day(1), 2017-12-08 23:00+10:00), where the entries have type TimeZones.ZonedDateTime.
"""
function parse_noncore_range(parser::Function, returntype::DataType, validvalues::String)
    @assert length(validvalues) >= 5  # "(a,b)" contains 5 characters
    @assert validvalues[1] == '('
    @assert validvalues[end] == ')'
    validvalues = strip.(String.(split(validvalues[2:(end-1)], ",")))
    length(validvalues) != 3 && error("Range of non-Base type requires 3 elements. It has $(length(validvalues)).")
    val1 = parser(validvalues[1])
    val2 = returntype <: Dates.TimeType ? eval(Meta.parse(validvalues[2])) : parser(validvalues[2])
    val3 = parser(validvalues[3])
    val1:val2:val3
end

end
