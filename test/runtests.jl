using Test
using Schemata
using DataFrames
using Dates
using TimeZones


################################################################################
### Test constructors 
cs = ColumnSchema(:customer_id, "Customer ID", Int, !CATEGORICAL, REQUIRED, UNIQUE, 1:1_000_000)
ts = TableSchema(:mytable, "My table", [cs], [:customer_id])
schema = Schema(:myschema, "My data set", Dict(:mytable => ts))

@test_throws ErrorException ColumnSchema(:customer_id, "Customer ID", Int, !CATEGORICAL, REQUIRED, UNIQUE, UInt)     # Int != UInt
@test_throws ErrorException ColumnSchema(:new, "Customer is new", Char, CATEGORICAL, REQUIRED, !UNIQUE, ["y", "n"])  # Char != String
@test_throws ErrorException TableSchema(:mytable, "My table", [cs], [:XXXcustomer_id])  # primary_key non-existent


################################################################################
### Compare DataFrame to Schema

# Schema
patientid = ColumnSchema(:patientid, "Patient ID",  UInt,   !CATEGORICAL, REQUIRED,  UNIQUE, UInt)
age       = ColumnSchema(:age,       "Age (years)", Int,    !CATEGORICAL, REQUIRED, !UNIQUE, Int)
dose      = ColumnSchema(:dose,      "Dose size",   String,  CATEGORICAL, REQUIRED, !UNIQUE, ["small", "medium", "large"])
fever     = ColumnSchema(:fever,     "Had fever",   Bool,    CATEGORICAL, REQUIRED, !UNIQUE, Bool)
ts        = TableSchema(:mytable, "My table", [patientid, age, dose, fever], [:patientid])
schema    = Schema(:fever, "Fever schema", Dict(:mytable => ts))

pid2 = ColumnSchema(:pid2, "Patient ID", UInt, !CATEGORICAL, !REQUIRED, UNIQUE, UInt)
@test_throws ErrorException TableSchema(:mytable, "My table", [pid2, age, dose, fever], [:pid2])  # Primary key not unique

# DataFrame
tbl = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],
    dose      = ["small", "medium", "large", "medium"],
    fever     = [false, true, true, false]
)

# Compare data to schema
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 3

# Modify data to comply with the schema
categorical!(tbl, [:dose, :fever])  # Ensure :dose and :fever contain categorical data
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 1

tbl[:patientid] = convert(Vector{UInt}, tbl[:patientid])
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 0

# Modify schema: Forbid tbl[:age] having values of 120 or above
schema.tables[:mytable].columns[:age].valid_values = 0:120

# Compare again
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 1

# Fix data: Attempt 1
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], false);
@test size(issues, 1) == 1
@test tbl[4, :age] == 444

# Fix data: Attempt 2
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test size(issues, 1) == 1
@test ismissing(tbl[4, :age]) == true

# Fix data: Attempt 3
tbl[4, :age] = 44
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 0

# Add a new column to the schema
zipcode = ColumnSchema(:zipcode, "Zip code", Int, CATEGORICAL, !REQUIRED, !UNIQUE, 10000:99999)
insert_column!(schema.tables[:mytable], zipcode)
@test schema.tables[:mytable].col_order[end] == :zipcode
@test haskey(schema.tables[:mytable].columns, :zipcode)
@test schema.tables[:mytable].columns[:zipcode] == zipcode

# Write the updated schema to disk
#schemafile = joinpath(dirname(pathof(Schemata)), "..", "test/schemata/fever_updated.yaml")
#writeschema(schemafile, schema)
#schema_from_disk = readschema(schemafile)
#@test schema == schema_from_disk

# Add a corresponding (non-compliant) column to the data
tbl[:zipcode] = ["11111", "22222", "33333", "NULL"];  # CSV file was supplied with "NULL" values, forcing eltype to be String.
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 2

# Fix the data
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test size(issues, 1) == 0


# Add a new column to the schema
datatype = Dict("type" => Date, "args" => "Y-m-d")
dosedate = ColumnSchema(:date, "Dose date", datatype, CATEGORICAL, !REQUIRED, !UNIQUE, datatype)
insert_column!(schema.tables[:mytable], dosedate)

# Add a corresponding (compliant) column to the data
tbl[:date] = ["2017-12-01", "2017-12-01", "2017-12-11", "2017-12-09"];
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 2
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test size(issues, 1) == 0

################################################################################
# Test ZonedDateTime
d = Dict("name"        => "zdt", "unique" => false, "required" => true, "description" => "descr","categorical" => false,
         "datatype"    => Dict("args"=>["Y-m-d H:M", "Australia/Melbourne"], "type"=>"TimeZones.ZonedDateTime"),
         "validvalues" => "(today()-Year(2), Day(1), today()+Year(1))")
cs     = ColumnSchema(d)
ts     = TableSchema(:mytable, "My table", [cs], [:zdt])
schema = Schema(:myschema, "My schema", Dict(:mytable => ts))

tbl = DataFrame(zdt=[DateTime(today()) + Hour(i) for i = 1:3])
target = [ZonedDateTime(tbl[i, :zdt], TimeZone("Australia/Melbourne")) for i = 1:3]
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test tbl[:zdt] == target

tbl = DataFrame(zdt=[string(DateTime(today()) + Hour(i)) for i = 1:3])  # String type
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test tbl[:zdt] == target

tbl = DataFrame(zdt=[string(ZonedDateTime(DateTime(today()) + Hour(i), TimeZone("Australia/Melbourne"))) for i = 1:3])  # String type
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
@test tbl[:zdt] == target

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
    issues = diagnose(d, schema.tables[:dates])
end
issues = test_row_constraints()
@test size(issues, 1) == 1
