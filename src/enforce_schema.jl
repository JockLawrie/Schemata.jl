"""
Returns: table, issues

The table is as compliant as possible with the schema.
If the table is completely compliant with the schema, then the issues table has 0 rows.
Otherwise the issues table contains the ways in which the output table does not comply with the schema.
"""
function enforce_schema(indata, tblschema::TableSchema, set_invalid_to_missing::Bool)
    n       = size(indata, 1)
    outdata = init_compliant_data(tblschema, n)
    issues  = NamedTuple{(:entity, :id, :issue),Tuple{String,String,String}}[]
    tblname = tblschema.name

    #=
      For each column, for each value:
      - Parse or convert value if necessary. If this is not possible, the value is invalid.
      - If value is invalid and set_invalid_to_missing = true, discard invalid value, else record the (colname,invalid_value) as an issue.
      - If final value is valid, copy into result.
      - Set categorical columns where required
    =#
    for (colname, colschema) in tblschema.columns
        !hasproperty(indata, colname) && continue  # Desired column not in indata; outdata will have a column of missings.
        target_type  = colschema.eltyp
        validvals    = colschema.valid_values
        vv_type      = typeof(validvals)
        invalid_vals = Set{Any}()
        for i = 1:n
            val = indata[i, colname]
            ismissing(val) && continue
            typeof(val) == String && val == "" && continue
            is_invalid = false
            if typeof(val) != target_type  # Convert type
                try
                    val = parse_as_type(target_type, val)
                catch
                    is_invalid = true
                end
            end
            # Value has correct type, now check that value is in the valid range
            if !is_invalid && (vv_type <: Vector || vv_type <: AbstractRange) && !value_is_valid(val, validvals)
                is_invalid = true
            end
            # Record invalid value
            if is_invalid && !set_invalid_to_missing
                push!(invalid_vals, val)
            end
            if !is_invalid || (is_invalid && !set_invalid_to_missing)
                if typeof(val) == nonmissingtype(eltype(outdata[!, colname]))
                    outdata[i, colname] = val
                end
            end
        end
        if colschema.is_categorical
            categorical!(outdata, colname)
        end
        if !isempty(invalid_vals)
            invalid_vals = [x for x in invalid_vals]  # Convert Set to Vector
            sort!(invalid_vals)
            push!(issues, (entity="column", id="$(tblname).$(colname)", issue="Invalid values: $(invalid_vals)"))
        end
    end

    # Get remaining issues from the output table. Combine these with those found earlier.
    other_issues = diagnose(outdata, tblschema)
    issues = unique(vcat(issues, other_issues))
    outdata, issues
end

value_is_valid(val, validvals::Vector) = in(val, validvals)
value_is_valid(val, validvals::AbstractRange) = isless(validvals[1], val) && isless(val, validvals[end])  #Check only the end points for efficiency. TODO: Check interior points efficiently.

function value_is_valid(val::T, validvals::AbstractRange) where {T <: CategoricalValue}
    isless(validvals[1], get(val)) && isless(get(val), validvals[end]) #Check only the end points for efficiency. TODO: Check interior points efficiently.
end


"Returns: A table with unpopulated columns with name, type, length and order matching the table schema."
function init_compliant_data(tblschema::TableSchema, n::Int)
    result = DataFrame()
    for colname in tblschema.col_order
        colschema = tblschema.columns[colname]
        eltyp     = eltype(colschema)
        result[!, colname] = missings(eltyp, n)
    end
    result
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
