function readschema(filename::String)
    # Read yaml
    io = open(filename)
    d  = YAML.load(io)
    close(io)
    length(d) > 1 && error("File $(filename) contains an incorrectly specified schema.")

    # Get schema name
    schema = ""
    schema_name = ""
    for (k, v) in d
        schema_name = k
        schema = v
        break
    end
    schema["name"] = schema_name
    Schema(schema)
end

################################################################################
### Utils
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


"""
Determine ColumnSchema.valid_values from vv and ColumSchema.eltyp (cs_eltyp).
"""
function determine_validvalues(vv, cs_eltyp)
    vv == "datatype" && return cs_eltyp  # Shortcut to set valid_values = eltyp
    determine_vv(vv, cs_eltyp)
end


"""
Returns: An instance of ColumnSchema.valid_values.

vv could be a:
- DataType. E.g., "Int64"
- Range of a Base type. E.g., "1:10"
- Range of a non-Base type, represented with a tuple: "(val1, val2)" or "(val1, stepsize, val2)"
"""
function determine_vv(vv::String, eltyp)
    vv[1] == '(' && return parse_nonbase_range(vv, eltyp)
    result = try
        eval(Meta.parse("$(parentmodule(@__MODULE__)).$(vv)"))  # Prepend module for types
    catch
        eval(Meta.parse(vv))  # Instances of types
    end
    result
end


function determine_vv(vv::Vector, eltyp)
    vv
end


"Used if valid_values is the same as eltyp, and `vv != datatype`."
function determine_vv(vv::Dict, eltyp)
    determine_eltype(vv)
end


"""
Used to represent ranges for non-Base types.

The format is: "(start, stop)" or "(start, stepsize, stop)"

If vv has 2 entries, these represent the end-points of the range; i.e., start:stop.
If vv has 3 entries, the middle entry represents the step size; i.e., start:stepsize:stop.

Example: (2017-10-01 09:00, 2017-12-08 23:00), where the entries have type TimeZones.ZonedDateTime.

For Base types a stringified range will work, E.g., `eval(Meta.parse("1:10"))` will return the range 1:10.
For non-Base types this approach will fail.   E.g., `eval(Meta.parse("2017-10-01 09:00:2017-12-08 23:00"))` will fail.
"""
function parse_nonbase_range(vv::String, eltyp)
    @assert length(vv) >= 5  # "(a,b)" contains 5 characters
    @assert vv[1] == '('
    @assert vv[end] == ')'
    #= The next line (from the inner-most operation):
       - Removes parentheses
       - Splits on comma...returns Vector{SubString}
       - Converts to Vector{String}
       - Removes leading/trailing spaces.
    =#
    vv = strip.(String.(split(vv[2:(end-1)], ",")))
    if length(vv) != 3
        error("Range of non-Base type requires 3 elements. It has $(length(vv)).")
    else
        val1 = parse_as_type(eltyp, vv[1])
        tp   = get_datatype(eltyp)
        val2 = tp <: Dates.TimeType ? eval(Meta.parse(vv[2])) : parse_as_type(eltyp, vv[2])  # HACK: middle value has type Dates.Period
        val3 = parse_as_type(eltyp, vv[3])
        return val1:val2:val3
    end
end
