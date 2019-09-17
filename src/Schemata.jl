module Schemata

export CATEGORICAL, REQUIRED, UNIQUE,     # constants
       Schema, TableSchema, ColumnSchema, # types
       diagnose, enforce_schema,          # functions
       insert_column!,                    # functions
       readschema #writeschema,           # functions

const CATEGORICAL = true
const REQUIRED    = true
const UNIQUE      = true

include("CustomParsers.jl")
include("handle_validvalues.jl")
include("schematypes.jl")
include("diagnose.jl")
include("enforce_schema.jl")
include("conveniences.jl")
include("readwrite.jl")

using .CustomParsers
using .handle_validvalues
using .schematypes
using .diagnosedata
using .enforce
using .conveniences
using .readwrite

#=
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
=#


end
