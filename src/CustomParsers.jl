"""
CustomParsers extends the functionality of the `Parsers` package by allowing users to provide custom parsers.

In particular, users can parse values with types that are not in Julia's `Core` module.
Users can also use the interface to parse `Core` types in non-standard ways, as well as in standard ways.

Calling `parse(parser, value)` returns a value with type `parser.returntype`.
"""
module CustomParsers

export CustomParser, parse

import Base.parse

using Dates
using Parsers


struct CustomParser
    func::Union{Function, DataType}  # Hack until Parsers.jl issue #38 is resolved and the solution released (then ensure func is a Function)
    args::Vector
    kwargs::Dict
    returntype::DataType

    function CustomParser(func, args, kwargs, returntype)
        func isa DataType && func != returntype && error("CustomParser: func is a DataType that doesn't equal returntype")
        if returntype == Date && length(args) == 1  # Hack: convert args = [dateformat::String] to args = [DateFormat(dateformat)]...parses faster
            args = [DateFormat(args[1])]
        end
        new(func, args, kwargs, returntype)
    end
end

function CustomParser(d::Dict)
    func   = haskey(d, "function") ? d["function"] : Parsers.parse
    args   = haskey(d, "args")     ? d["args"] : String[]  # Default String[] is arbitrary
    kwargs = haskey(d, "kwargs")   ? Dict(Symbol(k) => v for (k, v) in d["kwargs"]) : Dict{Symbol, Any}()
    CustomParser(func, args, kwargs, d["returntype"])
end

CustomParser(returntype::DataType) = CustomParser(Dict("returntype" => returntype))

function parse(parser::CustomParser, val)
    if parser.func == Parsers.parse  # Using Parsers (Core return type: parentmodule(parser.returntype) == Core)
        isempty(parser.kwargs) && return Parsers.parse(parser.returntype, val)
        return Parsers.parse(parser.returntype, val, Parsers.Options(parser.kwargs...))
    else                             # Using custom parser (Non-Core return type)
        isempty(parser.kwargs) && return parser.func(val, parser.args...)
        return parser.func(val, parser.args...; parser.kwargs...)
    end
end

end
