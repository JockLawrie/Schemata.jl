"""
CustomParsers extends the functionality of `Parsers` allowing users to provide custom parsers.

Calling `parse(parser, value)` returns a value with type `parser.returntype`.

In particular, users can parse values with types that are not in Julia's `Core` module.
Users can also use the interface to parse `Core` types in non-standard ways, as well as in standard ways.
"""
module CustomParsers

export CustomParser, parse

import Base.parse

using Parsers

struct CustomParser
    func::Function
    args::Vector
    kwargs::Dict
    returntype::DataType
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
