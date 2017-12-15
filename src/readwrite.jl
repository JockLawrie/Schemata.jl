function readschema(filename::String)
    # Read yaml
    io  = open(filename)
    dct = YAML.load(io)
    close(io)
    length(dct) > 1 && error("File $(filename) contains an incorrectly specified schema.")

    # Get schema name
    schema = ""
    schema_name = ""
    for (k, v) in dct
        schema_name = k
        schema = v
        break
    end
    schema["name"] = schema_name
    Schema(schema)
end

#=
function writeschema(filename::String, schema::Schema)
end
=#

################################################################################
### Utils
"""
Determine ColumnSchema.eltyp.
"""
function determine_eltype(s::String)
    eval(parse("$(module_parent(current_module())).$(s)"))  # Prepend module for non-Base types
end


function determine_eltype(d::Dict)
    d["type"] = eval(parse("$(module_parent(current_module())).$(d["type"])"))
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
    result = Vector{Any}(nargs)
    for i = 1:nargs
        try
            result[i] = eval(parse("$(module_parent(current_module())).$(vec[i])"))
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
    eval(parse("$(module_parent(current_module())).$(vv)"))  # Prepend module for non-Base types
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

For Base types a stringified range will work, E.g., `eval(parse("1:10"))` will return the range 1:10.
For non-Base types this approach will fail.   E.g., `eval(parse("2017-10-01 09:00:2017-12-08 23:00"))` will fail.
"""
function parse_nonbase_range(vv::String, eltyp)
    assert(length(vv) >= 5)  # "(a,b)" contains 5 characters
    assert(vv[1] == '(')
    assert(vv[end] == ')')
    #= The next line (from the inner-most operation):
       - Removes parentheses
       - Splits on comma...returns Vector{SubString}
       - Converts to Vector{String}
       - Removes leading/trailing spaces.
    =#
    vv = strip.(String.(split(vv[2:(end-1)], ",")))
    if length(vv) == 1
        error("Range of non-Base type contains only 1 element.")
    elseif length(vv) == 2
        val1 = parse_as_type(eltyp, vv[1])
        val2 = parse_as_type(eltyp, vv[2])
        return val1:Dates.Minute(1):val2
    elseif length(vv) == 3
        val1 = parse_as_type(eltyp, vv[1])
        val2 = parse_as_type(eltyp, vv[2])
        val3 = parse_as_type(eltyp, vv[3])
        return val1:val2:val3
    else
        error("Range of non-Base type contains more than 3 elements.")
    end
end