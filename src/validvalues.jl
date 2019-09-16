get_datatype(validvalues::DataType) = validvalues
get_datatype(validvalues::Set)      = eltype(validvalues)
get_datatype(validvalues::T) where {T <: AbstractRange} = typeof(validvalues[1])

#=
"""
Determine ColumnSchema.eltyp.
"""
function determine_eltype(s::String)
    #eval(Meta.parse("$(parentmodule(@__MODULE__)).$(s)"))  # Prepend module for non-Base types
    eval(Meta.parse(s))  # Type is in Base
end


function determine_eltype(d::Dict)
    d["type"] = try
        eval(Meta.parse("$(parentmodule(@__MODULE__)).$(d["type"])"))
    catch
        eval(Meta.parse("import $(d["type"])"))
    end
    if haskey(d, "args")
        d["args"] = convert_args_types(d["args"])
    end
    if haskey(d, "kwargs")
        d["kwargs"] = convert_args_types(d["kwargs"])
    end
    d
end


function convert_args_types(vec::Vector)
    nargs  = length(vec)
    result = Vector{Any}(undef, nargs)
    for i = 1:nargs
        try
            #result[i] = eval(Meta.parse("$(parentmodule(@__MODULE__)).$(vec[i])"))
            result[i] = eval(Meta.parse(vec[i]))
        catch
            result[i] = vec[i]
        end
    end
    result
end
=#


"""
Determine ColumnSchema.validvalues from validvalues and ColumSchema.eltyp (cs_eltyp).
"""
function determine_validvalues(validvalues, cs_eltyp)
    validvalues == "datatype" && return cs_eltyp  # Shortcut to set valid_values = eltyp
    determine_validvalues(validvalues, cs_eltyp)
end


"""
Returns: An instance of ColumnSchema.validvalues.

validvalues could be a:
- DataType. E.g., "Int64"
- Range of a Base type. E.g., "1:10"
- Range of a non-Base type, represented with a tuple: "(val1, val2)" or "(val1, stepsize, val2)"
"""
function determine_validvalues(validvalues::String, eltyp)
    validvalues[1] == '(' && return parse_nonbase_range(validvalues, eltyp)
    result = try
        eval(Meta.parse("$(parentmodule(@__MODULE__)).$(validvalues)"))  # Prepend module for types
    catch
        eval(Meta.parse(validvalues))  # Instances of types
    end
    result
end


function determine_validvalues(validvalues::Vector, eltyp)
    eltype(validvalues) == eltyp ? validvalues : eltyp.(validvalues)
end


"Used if valid_values is the same as eltyp, and `validvalues != datatype`."
function determine_validvalues(validvalues::Dict, eltyp)
    determine_eltype(validvalues)
end


"""
Used to represent ranges for non-Base types.

The format is: "(start, stop)" or "(start, stepsize, stop)"

If validvalues has 2 entries, these represent the end-points of the range; i.e., start:stop.
If validvalues has 3 entries, the middle entry represents the step size; i.e., start:stepsize:stop.

Example: (2017-10-01 09:00, 2017-12-08 23:00), where the entries have type TimeZones.ZonedDateTime.

For Base types a stringified range will work, E.g., `eval(Meta.parse("1:10"))` will return the range 1:10.
For non-Base types this approach will fail.   E.g., `eval(Meta.parse("2017-10-01 09:00:2017-12-08 23:00"))` will fail.
"""
function parse_nonbase_range(validvalues::String, eltyp)
    @assert length(validvalues) >= 5  # "(a,b)" contains 5 characters
    @assert validvalues[1] == '('
    @assert validvalues[end] == ')'
    #= The next line (from the inner-most operation):
       - Removes parentheses
       - Splits on comma...returns Vector{SubString}
       - Converts to Vector{String}
       - Removes leading/trailing spaces.
    =#
    validvalues = strip.(String.(split(validvalues[2:(end-1)], ",")))
    if length(validvalues) != 3
        error("Range of non-Base type requires 3 elements. It has $(length(validvalues)).")
    else
        val1 = parse_as_type(eltyp, validvalues[1])
        tp   = get_datatype(eltyp)
        val2 = tp <: Dates.TimeType ? eval(Meta.parse(validvalues[2])) : parse_as_type(eltyp, validvalues[2])  # HACK: middle value has type Dates.Period
        val3 = parse_as_type(eltyp, validvalues[3])
        return val1:val2:val3
    end
end

function parse_as_type(::Type{Date}, val::T) where {T <: AbstractString}
    try
        Date(val[1:10])   # example: "2017-12-31"
    catch
        eval(Meta.parse(val))  # example: "today() + Day(4)"
    end
end

function parse_as_type(target_type::T, val) where {T <: Dict}
    tp = target_type["type"]
    if haskey(target_type, "kwargs")
        tp(val, target_type["args"]...; target_type["kwargs"]...)
    else
        tp(val, target_type["args"]...)
    end
end

function parse_as_type(target_type, val)
    try
        parse(target_type, val)
    catch
        convert(target_type, val)
    end
end


# This block enables a custom constructor of an existing non-Base type (TimeZones.ZonedDateTime)
function TimeZones.ZonedDateTime(dt::T, fmt::String, tz::TimeZones.TimeZone) where {T <: AbstractString}
    i = Int(Char(dt[1]))
    if i >= 48 && i <= 57  # dt[1] is a digit in 0,1,...,9.
        if !occursin("T", fmt)
            fmt = replace(fmt, " " => "T")              # Example: old value: "Y-m-d H:M"; new value: "Y-m-dTH:M"
        end
        if !occursin("T", dt)
            dt = replace(dt, " " => "T")                # Example: old value: "2017-12-31T09:29"; new value: "2017-12-31 09:29"
        end

        # Remove existing TimeZone
        idx = findfirst(isequal('+'), dt)  # Example: "2016-11-08T13:15:00+11:00"
        if idx != nothing
            dt = dt[1:(idx-1)]             # Example: "2016-11-08T13:15:00"
        end

        # Convert String to DateTime
        dttm = try
            DateTime(dt)
        catch
            DateTime(dt, fmt)
        end
        TimeZones.ZonedDateTime(dttm, tz)  # Example: dt = "2017-12-31 09:29"
    else
        TimeZones.ZonedDateTime(DateTime(eval(Meta.parse(dt))), tz)  # Example: dt = "today() + Day(2)"
    end
end
TimeZones.ZonedDateTime(dt::T, fmt::String, tz::String) where {T <: AbstractString} = ZonedDateTime(dt, fmt, TimeZone(tz))
TimeZones.ZonedDateTime(dt::DateTime, fmt::String, tz) = ZonedDateTime(dt, tz)
TimeZones.ZonedDateTime(dt::DateTime, tz::String)      = ZonedDateTime(dt, TimeZone(tz))
TimeZones.ZonedDateTime(dt::Date, fmt::String, tz)     = ZonedDateTime(DateTime(dt), tz)
