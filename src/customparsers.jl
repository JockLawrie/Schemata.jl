"""
CustomParsers extends the functionality of `Parsers` allowing users to provide custom parsers.

Calling `parse(parser, value)` returns a value with type `parser.returntype`.

In particular, users can parse values with types that are not in Julia's `Core` module.
Users can also use the interface to parse `Core` types in non-standard ways, as well as in standard ways.
"""
module customparsers

export CustomParser, parse

using Parsers

struct CustomParser
    func::Function
    args::Vector
    kwargs::Dict
    returntype::DataType
end

function CustomParser(d::Dict, datatype::DataType)
    func   = haskey(d, "parser") ? d["parser"] : Parsers.parse
    args   = haskey(d, "args")   ? d["args"] : String[]  # Default String[] is arbitrary
    kwargs = haskey(d, "kwargs") ? Dict(Symbol(k) => v for (k, v) in d["kwargs"]) : Dict{Symbol, Any}()
    Parser(func, args, kwargs, datatype)
end

function CustomParser(datatype::DataType)
    func   = Parsers.parse
    args   = String[]
    kwargs = Dict{Symbol, Any}()
    Parser(func, args, kwargs, datatype)
end

function parse(parser::CustomParser, val)
    # Using Parsers (Core return type: parentmodule(parser.returntype) == Core)
    if parser.func == Parsers.parse
        if isempty(parser.kwargs)
            return parser.func(parser.returntype, val)
        else
            return parser.func(parser.returntype, val, Parsers.Options(parser.kwargs...))
        end
    end

    # Using custom parser (Non-Core return type)
    isempty(parser.kwargs) && return parser.func(parser.returntype, val, parser.args...)
    parser.func(parser.returntype, val, parser.args...; parser.kwargs...)
end

end
