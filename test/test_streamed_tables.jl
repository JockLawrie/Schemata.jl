################################################################################
### Construct table and write to disk

infile  = "my_test_table_in.csv"
outfile = "my_test_table_out.csv"

tbl = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],
    dose      = ["small", "medium", "large", "medium"],
    fever     = [false, true, true, false]
)

CSV.write(infile, tbl; delim=',')
tbl = nothing

################################################################################
### Compare DataFrame to Schema

# Schema
patientid = ColumnSchema(:patientid, "Patient ID",  UInt,   !CATEGORICAL, REQUIRED,  UNIQUE, UInt)
age       = ColumnSchema(:age,       "Age (years)", Int,    !CATEGORICAL, REQUIRED, !UNIQUE, Int)
dose      = ColumnSchema(:dose,      "Dose size",   String,  CATEGORICAL, REQUIRED, !UNIQUE, ["small", "medium", "large"])
fever     = ColumnSchema(:fever,     "Had fever",   Bool,    CATEGORICAL, REQUIRED, !UNIQUE, Bool)
ts        = TableSchema(:mytable, "My table", [patientid, age, dose, fever], [:patientid])

# Compare data to schema
issues = diagnose(infile, ts)
@test size(issues, 1) == 0

# Modify schema: Forbid the age columns from having values of 120 or above
age.validvalues = 0:120

# Compare again
issues = diagnose(infile, ts)
@test size(issues, 1) == 1

# Fix data: Attempt 1
issues  = enforce_schema(infile, ts, false, outfile);
outdata = DataFrame(CSV.File(outfile))
@test size(issues, 1) == 1
@test outdata[4, :age] == 444

# Fix data: Attempt 2
issues  = enforce_schema(infile, ts, true, outfile);
outdata = DataFrame(CSV.File(outfile))
@test size(issues, 1) == 1
@test ismissing(outdata[4, :age]) == true

# Fix data: Attempt 3
indata = DataFrame(CSV.File(infile))
indata[4, :age] = 44
CSV.write(infile, indata; delim=',')
issues = diagnose(infile, ts)
@test size(issues, 1) == 0

################################################################################
# Test intra-row constraints
function test_row_constraints()
    filename = joinpath(dirname(pathof(Schemata)), "..", "test/schemata/row_constraints.yaml")
    schema   = readschema(filename)
    indata   = DataFrame(
                  patientid = UInt.([1,2,3]),
                  dob=Date.(["1992-10-01", "1988-03-23", "1983-11-18"]),
                  date_of_marriage=[Date("2015-09-13"), missing, Date("1981-11-01")])
    CSV.write(infile, indata; delim=',')
    issues = diagnose(infile, schema.tables[:dates])
end
issues = test_row_constraints()
@test size(issues, 1) == 1

################################################################################
# Clean up

rm(infile)
rm(outfile)
