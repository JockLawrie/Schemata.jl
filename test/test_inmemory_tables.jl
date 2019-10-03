################################################################################
### Test constructors 

cs     = ColumnSchema(:customer_id, "Customer ID", Int, !CATEGORICAL, REQUIRED, UNIQUE, 1:1_000_000)
ts     = TableSchema(:mytable, "My table", [cs], [:customer_id])
schema = Schema(:myschema, "My data set", Dict(:mytable => ts))

@test_throws ErrorException ColumnSchema(:customer_id, "Customer ID", Int, !CATEGORICAL, REQUIRED, UNIQUE, UInt)     # Int != UInt
@test_throws ErrorException ColumnSchema(:new, "Customer is new", Char, CATEGORICAL, REQUIRED, !UNIQUE, ["y", "n"])  # Char != String
@test_throws ErrorException TableSchema(:mytable, "My table", [cs], [:XXXcustomer_id])  # primarykey non-existent


################################################################################
### Compare DataFrame to Schema

# Schema
patientid = ColumnSchema(:patientid, "Patient ID",  UInt,   !CATEGORICAL, REQUIRED,  UNIQUE, UInt)
age       = ColumnSchema(:age,       "Age (years)", Int,    !CATEGORICAL, REQUIRED, !UNIQUE, Int)
dose      = ColumnSchema(:dose,      "Dose size",   String,  CATEGORICAL, REQUIRED, !UNIQUE, ["small", "medium", "large"])
fever     = ColumnSchema(:fever,     "Had fever",   Bool,    CATEGORICAL, REQUIRED, !UNIQUE, Bool)
ts        = TableSchema(:mytable, "My table", [patientid, age, dose, fever], [:patientid])
schema    = Schema(:fever, "Fever schema", Dict(:mytable => ts))

pid2 = ColumnSchema(:pid2, "Patient ID - version 2", UInt, !CATEGORICAL, !REQUIRED, UNIQUE, UInt)
@test_throws ErrorException TableSchema(:mytable, "My table", [pid2, age, dose, fever], [:pid2])  # Primary key not unique

# DataFrame
tbl = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],
    dose      = ["small", "medium", "large", "medium"],
    fever     = [false, true, true, false]
)

# Compare data to schema
outdata, issues_in, issues_out = compare(ts, tbl)
@test size(issues_in, 1) == 4

# Modify data to comply with the schema
categorical!(tbl, [:dose, :fever])  # Ensure :dose and :fever contain categorical data
outdata, issues_in, issues_out = compare(ts, tbl)
@test size(issues_in,  1) == 2
@test size(issues_out, 1) == 0

tbl[!, :patientid] = convert(Vector{UInt}, tbl[!, :patientid])
outdata, issues_in, issues_out = compare(ts, tbl)
@test size(issues_in, 1) == 0

# Modify schema: Forbid tbl[:age] having values of 120 or above
schema.tables[:mytable].colname2colschema[:age].validvalues = 0:120

# Compare again
outdata, issues_in, issues_out = compare(schema.tables[:mytable], tbl)
@test size(issues_in, 1)  == 1
@test size(issues_out, 1) == 1
@test ismissing(outdata[4, :age]) == true

# Fix data
tbl[4, :age] = 44
outdata, issues_in, issues_out = compare(schema.tables[:mytable], tbl)
@test size(issues_in, 1) == 0

# Add a new column to the schema
zipcode = ColumnSchema(:zipcode, "Zip code", Int, !CATEGORICAL, !REQUIRED, !UNIQUE, 10000:99999)
schema.tables[:mytable].colname2colschema[:zipcode] = zipcode
push!(schema.tables[:mytable].columnorder, :zipcode)
@test schema.tables[:mytable].columnorder[end] == :zipcode
@test haskey(schema.tables[:mytable].colname2colschema, :zipcode)
@test schema.tables[:mytable].colname2colschema[:zipcode] == zipcode

# Write the updated schema to disk
#schemafile = joinpath(dirname(pathof(Schemata)), "..", "test/schemata/fever_updated.yaml")
#writeschema(schemafile, schema)
#schema_from_disk = readschema(schemafile)
#@test schema == schema_from_disk

# Add a corresponding (non-compliant) column to the data
tbl[!, :zipcode] = ["11111", "22222", "33333", "NULL"];  # CSV file was supplied with "NULL" values, forcing eltype to be String.
outdata, issues_in, issues_out = compare(schema.tables[:mytable], tbl)
@test size(issues_in, 1)  == 2
@test size(issues_out, 1) == 0

# Add a new column to the schema
dosedate = ColumnSchema(:date, "Dose date", Date, !CATEGORICAL, !REQUIRED, !UNIQUE, Date)
schema.tables[:mytable].colname2colschema[:date] = dosedate
push!(schema.tables[:mytable].columnorder, :date)

# Add a corresponding (compliant) column to the data
tbl[!, :date] = ["2017-12-01", "2017-12-01", "2017-12-11", "2017-12-09"];
outdata, issues_in, issues_out = compare(schema.tables[:mytable], tbl)
@test size(issues_in, 1)  == 4
@test size(issues_out, 1) == 0

################################################################################
# Test CustomParser

# Define custom parser
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
cs = ColumnSchema(d)
ts = TableSchema(:mytable, "My table", [cs], [:zdt])

tbl = DataFrame(zdt=[DateTime(today() - Day(7)) + Hour(i) for i = 1:3])
target = [ZonedDateTime(tbl[i, :zdt], TimeZone("Australia/Melbourne")) for i = 1:3]
outdata, issues_in, issues_out = compare(ts, tbl);
@test outdata[!, :zdt] == target

tbl = DataFrame(zdt=[string(DateTime(today() - Day(7)) + Hour(i)) for i = 1:3])  # String type
outdata, issues_in, issues_out = compare(ts, tbl);
@test outdata[!, :zdt] == target

tbl = DataFrame(zdt=[string(ZonedDateTime(DateTime(today() - Day(7)) + Hour(i), TimeZone("Australia/Melbourne"))) for i = 1:3])  # String type
outdata, issues_in, issues_out = compare(ts, tbl);
@test outdata[!, :zdt] == target

################################################################################
# Test conversion of String arg to DateFormat when the datatype is Date.

d = Dict("name"        => "dt",
         "description" => "Test Date parser with a prebuilt DateFormat stored in args, rather than construct a DateFormat for each parsing",
         "datatype"    => "Date", "iscategorical" => false, "isrequired" => true, "isunique" => true,
         "validvalues" => "(01JAN2019, Day(1), 31DEC2019)",
         "parser"      => Dict("function" => "Date", "args"=>["dduuuyyyy"]))

cs  = ColumnSchema(d)
ts  = TableSchema(:mytable, "My table", [cs], [:dt])
tbl = DataFrame(dt=["02DEC2019", "03DEC2019", "04DEC2019"])
outdata, issues_in, issues_out = compare(ts, tbl);
@test outdata[!, :dt] == [Date(2019, 12, 1) + Day(i) for i = 1:3]

################################################################################
# Test intra-row constraints
function test_row_constraints()
    filename = joinpath(dirname(pathof(Schemata)), "..", "test/schemata/row_constraints.yaml")
    schema   = readschema(filename)
    d = DataFrame(
                  patientid = UInt.([1,2,3]),
                  dob=Date.(["1992-10-01", "1988-03-23", "1983-11-18"]),
                  date_of_marriage=[Date("2015-09-13"), missing, Date("1981-11-01")]
                 )
    compare(schema.tables[:dates], d)
end
outdata, issues_in, issues_out = test_row_constraints()
@test size(issues_in, 1) == 1
