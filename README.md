# Schemata.jl

A `Schema` is a specification of a data set, which may contain more than 1 table.

It exists independently of any particular data set, and therefore can be constructed in the absence of a data set.

This package facilitates 3 use cases:

1. Read/write a schema from/to a yaml file.

2. Compare a data set to a schema and list the non-compliance issues.

3. Transform a data set to comply with a schema as much as possible and return a table of outstanding issues.

Although a schema can be specified in code, it is more practical to specify it in a configuration file.
This has the added benefit of sharing it with non-technical data custodians and researchers,
providing a common language for discussing data-related issues.
Indeed the 3 use cases listed above can be carried out without writing any Julia code - just call one of the scripts in the `scripts` directory.

# Usage

A `TableSchema` looks like this `yaml` file:

```YAML
name: mytable
description: "My table"
primarykey: patientid  # A column name or a vector of column names
columns:
  - patientid: {description: Patient ID,  datatype: UInt,   iscategorical: false, isrequired: true, isunique: true,  validvalues: UInt}
  - age:       {description: Age (years), datatype: Int,    iscategorical: false, isrequired: true, isunique: false, validvalues: "0:120"}
  - dose:      {description: Dose size,   datatype: String, iscategorical: true,  isrequired: true, isunique: false, 
                validvalues: ["small", "medium", "large"]
  - fever:     {description: Had fever,   datatype: Bool,   iscategorical: true,  isrequired: true, isunique: false, validvalues: Bool}
```

A `Schema` contains 1 or more `TableSchema`. For example:

```YAML
name: fever
description: "Fever schema"
tables:
  table1: *table1_schema
  table2: *table2_schema
```

For tables that fit into memory, usage is as follows:

```julia
# Read in a schema
using Schemata

schema = readschema(joinpath(dirname(pathof(Schemata)), "..", "test/schemata/fever.yaml"))
ts     = schema.tables[:mytable]  # TableSchema for mytable

# Construct/import a table (any object that satisfies the Tables.jl interface)
using DataFrames

table = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],  # Note that 444 is not in the set of valid values according to the schema
    dose      = ["small", "medium", "large", "medium"],
    fever     = [false, true, true, false]
)

# Transform the table to comply with the schema.
# Values that are unparseable or invalid are set to missing.
# Return the transformed data, a table of input data issues and a table of output data issues.
outdata, input_issues, output_issues = diagnose(ts, table)
```

For tables that are too big to fit into memory, replace the table argument with the filename of the table:

```julia
# Transform the table to comply with the schema.
# Values that are unparseable or invalid are set to missing.
# Write the transformed data, a table of input data issues and a table of output data issues to disk.
input_data_file    = "/path/to/mytable.tsv"
output_data_file   = "/path/to/transformed_table.tsv"
input_issues_file  = "/path/to/input_issues.tsv"
output_issues_file = "/path/to/output_issues.tsv"
diagnose(ts, input_data_file, output_data_file, input_issues_file, output_issues_file)

# Or simply...
diagnose(ts, input_data_file)  # output_data_file, input_issues_file, output_issues_file have default values
```

# Custom Parsers

The `CustomParsers` submodule allows users to provide custom parsers.
This allows users to parse:
- Values with types that are not in Julia's `Core` module.
- Values of `Core` types in non-standard ways, such as custom date formats.
- Values of `Core` types in standard ways with a unified interface.

A `CustomParser` has the form:

```julia
struct CustomParser
    func::Union{Function, DataType}
    args::Vector
    kwargs::Dict
    returntype::DataType
end
```

Calling `parse(myparser, value)` returns a value with type `myparser.returntype`.

A `CustomParser` can be constructed from a `Dict`, and therefore can be specified in a configuration file.
For example, the following code from the test suite defines a `CustomParser` for a `ZonedDateTime`.
Note the specification of a range of non-`Core` types, namely `(startvalue, stepsize, endvalue)`.

```julia
# Define custom parser
using TimeZones

function my_zdt_parser(s::T, tz::String) where {T <: AbstractString}
    occursin(':', s) && return ZonedDateTime(DateTime(s[1:16]), TimeZone(tz))  # Example: s="2020-12-31T09:30:59+10:00"
    dt = Date(eval(Meta.parse(s)))  # Examples: s="today()", s="2020-11-01"
    ZonedDateTime(DateTime(dt), TimeZone(tz))
end

my_zdt_parser(dttm::DateTime, tz::String) = ZonedDateTime(dttm, TimeZone(tz))

# Dict for ColumnSchema constructor, obtained after reading yaml
d = Dict("name"          => "zdt", "description" => "Test custom parser for TimeZones.ZonedDateTime",
         "datatype"      => "ZonedDateTime",
         "iscategorical" => false, "isrequired" => true, "isunique" => true,
         "validvalues"   => "(today()-Year(2), Hour(1), today()-Day(1))",  # Ensure that the range has sufficient resolution
         "parser"        => Dict("function" => "my_zdt_parser", "args"=>["Australia/Melbourne"]))

# Need to eval datatype and parser.function in the same scope that they were defined (and before constructing the ColumnSchema).
# Schemata.jl can't see the datatype and parser.function until it receives them from the current scope.
d["datatype"] = eval(Meta.parse(d["datatype"]))
d["parser"]["function"] = eval(Meta.parse(d["parser"]["function"]))

# Now the schema constructors can be used
cs = ColumnSchema(d)
ts = TableSchema(:mytable, "My table", [cs], [:zdt])

table  = DataFrame(zdt=[DateTime(today() - Day(7)) + Hour(i) for i = 1:3])
target = [ZonedDateTime(table[i, :zdt], TimeZone("Australia/Melbourne")) for i = 1:3]
outdata, issues_in, issues_out = diagnose(ts, table)
outdata[!, :zdt] == target

table = DataFrame(zdt=[string(DateTime(today() - Day(7)) + Hour(i)) for i = 1:3])  # String type
outdata, issues_in, issues_out = diagnose(ts, table)
outdata[!, :zdt] == target

table = DataFrame(zdt=[string(ZonedDateTime(DateTime(today() - Day(7)) + Hour(i), TimeZone("Australia/Melbourne"))) for i = 1:3])  # String type
outdata, issues_in, issues_out = diagnose(ts, table)
outdata[!, :zdt] == target
```

# Intra-Row Constraints

We often want to ensure that certain relationships hold between variables within a row.
For example, we might require that a person's marriage date is after his/her birth date.
We can achieve this by specifying one or more intra-row constraints in a `TableSchema` as follows:

```yaml
name: intrarow_constraints_demo
description: "Table with intra-row constraints"
primarykey: id
intrarow_constraints:
  birth date before marriage date: "r[:dob] < r[:date_of_marriage]"
columns:
  - id:  {description: ID, datatype: UInt, iscategorical: false, isrequired: true, isunique: true, validvalues: UInt}
  - dob: {description: Date of birth, datatype: Date, iscategorical: false, isrequired: true, isunique: false, validvalues: Date}
  - date_of_marriage: {description: Date of marriage, datatype: Date, iscategorical: false, isrequired: false, isunique: false, validvalues: Date}
```

Each constraint is specified as a key-value pair, where the key is a description of the constraint and
the value is the right-hand side of a function of a row `r`.
The function must return `true` or `false`.
When comparing the schema to a table, the function is executed on each row.
If the function returns `false` for one or more rows, the constraint isn't satisfied and its description is recorded in the returned issues table.
