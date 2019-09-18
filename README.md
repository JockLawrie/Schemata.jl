# Schemata.jl

A `Schema` is a specification of a data set.

It exists independently of any particular data set, and therefore can be constructed and modified in the absence of a data set.

This package facilitates 3 use cases:

1. Read/write a schema from/to a yaml file. Thus schemata are portable.

2. Compare a data set to a schema and list the non-compliance issues.

3. Transform an existing data set in order to comply with a schema as much as possible (then rerun the compare function to see any outstanding issues).


# Usage

```julia
# Read in a schema
using Schemata

schema = readschema(joinpath(dirname(pathof(Schemata)), "..", "test/schemata/fever.yaml"))

# Alternatively, construct the schema within the code
patientid = ColumnSchema(:patientid, "Patient ID",  UInt,   !CATEGORICAL, REQUIRED,  UNIQUE, UInt)
age       = ColumnSchema(:age,       "Age (years)", Int,    !CATEGORICAL, REQUIRED, !UNIQUE, Int)
dose      = ColumnSchema(:dose,      "Dose size",   String,  CATEGORICAL, REQUIRED, !UNIQUE, ["small", "medium", "large"])
fever     = ColumnSchema(:fever,     "Had fever",   Bool,    CATEGORICAL, REQUIRED, !UNIQUE, Bool)
ts        = TableSchema(:mytable, "My table", [patientid, age, dose, fever], [:patientid])
schema    = Schema(:fever, "Fever schema", Dict(:mytable => ts))

# Import some data
using DataFrames

tbl = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],
    dose      = ["small", "medium", "large", "medium"],
    fever     = [false, true, true, false]
)

# Compare the data to the schema
diagnose(tbl, schema.tables[:mytable])

# Modify the data to comply with the schema
categorical!(tbl, [:dose, :fever])        # Make these columns categorical
tbl[:patientid] = UInt.(tbl[:patientid])  # Change the data type from Int to UInt

# Compare again
diagnose(tbl, schema.tables[:mytable])

# Modify the schema: Require :age <= 120
schema.tables[:mytable].columns[:age].valid_values = 0:120

# Compare again
diagnose(tbl, schema.tables[:mytable])  # Looks like a data entry error

# Fix the data: Attempt 1 (do not set invalid values to missing)
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], false);
tbl
issues

# Fix the data: Attempt 2 (set invalid values to missing)
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
tbl
issues

# Fix the data: Attempt 3 (manually fix data entry error)
tbl[4, :age] = 44
diagnose(tbl, schema.tables[:mytable])

# Add a new column to the schema
zipcode = ColumnSchema(:zipcode, "Zip code", Int, CATEGORICAL, !REQUIRED, !UNIQUE, 10000:99999)
insert_column!(schema.tables[:mytable], zipcode)

# Add a corresponding (non-compliant) column to the data
tbl[:zipcode] = ["11111", "22222", "33333", "NULL"];  # CSV file was supplied with "NULL" values, forcing eltype to be String.
diagnose(tbl, schema.tables[:mytable])

# Fix the data
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
tbl
issues

# Add a Date column to the schema; note the args in the datatype
using Dates

datatype = Dict("type" => Date, "args" => "Y-m-d")
dosedate = ColumnSchema(:date, "Dose date", datatype, CATEGORICAL, !REQUIRED, !UNIQUE, datatype)
insert_column!(schema.tables[:mytable], dosedate)

# Add a corresponding (compliant) column to the data
tbl[:date] = ["2017-12-01", "2017-12-01", "2017-12-11", "2017-12-09"];
diagnose(tbl, schema.tables[:mytable])
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
show(tbl, true)
issues
```

# Custom Parsers

The submodule `CustomParsers` extends the functionality of the `Parsers` package by allowing users to provide custom parsers.
In particular, users can parse values with types that are not in Julia's `Core` module.
Users can also use the interface to parse `Core` types in non-standard ways, as well as in standard ways.

A `CustomParser` has the form:

```julia
struct CustomParser
    func::Function
    args::Vector
    kwargs::Dict
    returntype::DataType
end
```

Calling `parse(my_custom_parser, value)` returns a value with type `my_custom_parser.returntype`.

A `CustomParser` can be constructed from a `Dict`, and therefore can be specified in a config (yaml) file.
For example, the following code (from the test suite) defines a `CustomParser` for a `ZonedDateTime` (from the `TimeZones` package).

```julia
# Define custom parser
using TimeZones

function my_zdt_custom_parser(s::T, tz::String) where {T <: AbstractString}
    occursin(':', s) && return ZonedDateTime(DateTime(s[1:16]), TimeZone(tz))  # Example: s="2020-12-31T09:30:59+10:00"
    dt = Date(eval(Meta.parse(s)))  # Examples: s="today()", s="2020-11-01"
    ZonedDateTime(DateTime(dt), TimeZone(tz))
end

my_zdt_custom_parser(dttm::DateTime, tz::String) = ZonedDateTime(dttm, TimeZone(tz))

# Dict for ColumnSchema constructor, obtained after reading yaml
d = Dict("name"          => "zdt", "description" => "Test custom parser for TimeZones.ZonedDateTime",
         "datatype"      => "ZonedDateTime",
         "iscategorical" => false, "isrequired" => true, "isunique" => true,
         "validvalues"   => "(today()-Year(2), Hour(1), today()-Day(1))",  # Ensure that the range has sufficient resolution
         "parser"        => Dict("function" => "my_zdt_custom_parser", "args"=>["Australia/Melbourne"]))

# Need to eval datatype and parser.function in the same scope that they were defined (and before constructing the ColumnSchema).
# Schemata.jl can't see the datatype and parser.function until it receives them from the current scope.
d["datatype"] = eval(Meta.parse(d["datatype"]))
d["parser"]["function"] = eval(Meta.parse(d["parser"]["function"]))

# Now the schema constructors can be used
cs     = ColumnSchema(d)
ts     = TableSchema(:mytable, "My table", [cs], [:zdt])
schema = Schema(:myschema, "My schema", Dict(:mytable => ts))

tbl = DataFrame(zdt=[DateTime(today() - Day(7)) + Hour(i) for i = 1:3])
target = [ZonedDateTime(tbl[i, :zdt], TimeZone("Australia/Melbourne")) for i = 1:3]
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test tbl[!, :zdt] == target

tbl = DataFrame(zdt=[string(DateTime(today() - Day(7)) + Hour(i)) for i = 1:3])  # String type
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test tbl[!, :zdt] == target

tbl = DataFrame(zdt=[string(ZonedDateTime(DateTime(today() - Day(7)) + Hour(i), TimeZone("Australia/Melbourne"))) for i = 1:3])  # String type
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test tbl[!, :zdt] == target
```


# TODO

1. Implement `writeschema` (requires a write function to be implemented in `YAML.jl`).

2. Define joins between tables within a schema, which induce `intrarow_constraints` across tables.

3. Infer a `Schema` from a given data table.

4. Replace the dependence on DataFrames with dependence on the `Tables` interface.

5. Enable diagnosis/enforcement of tables that don't fit into memory.
