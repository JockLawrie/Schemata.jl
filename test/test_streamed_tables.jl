################################################################################
### Construct table and write to disk

data_infile    = "my_test_table_in.csv"
data_outfile   = "my_test_table_out.csv"
issues_infile  = "my_test_table_input_issues.tsv"
issues_outfile = "my_test_table_output_issues.tsv"

tbl = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],
    dose      = ["small", "medium", "large", "medium"],
    fever     = [false, true, true, false]
)

CSV.write(data_infile, tbl; delim=',')
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
compare(ts, data_infile; output_data_file=data_outfile, input_issues_file=issues_infile, output_issues_file=issues_outfile)
issues_in = DataFrame(CSV.File(issues_infile; delim='\t'))
@test size(issues_in, 1) == 0

# Modify schema: Forbid the age column from having values of 120 or above
age.validvalues = 0:120

# Compare again
compare(ts, data_infile; output_data_file=data_outfile, input_issues_file=issues_infile, output_issues_file=issues_outfile)
outdata    = DataFrame(CSV.File(data_outfile))
issues_in  = DataFrame(CSV.File(issues_infile; delim='\t'))
issues_out = DataFrame(CSV.File(issues_outfile; delim='\t'))
@test size(issues_in, 1)  == 1
@test size(issues_out, 1) == 1
@test ismissing(outdata[4, :age]) == true

# Fix input data
indata = DataFrame(CSV.File(data_infile))
indata[4, :age] = 44
CSV.write(data_infile, indata; delim=',')
compare(ts, data_infile; output_data_file=data_outfile, input_issues_file=issues_infile, output_issues_file=issues_outfile)
issues_in = DataFrame(CSV.File(issues_infile; delim='\t'))
@test size(issues_in, 1) == 0

################################################################################
# Test intra-row constraints
function test_row_constraints()
    filename = joinpath(dirname(pathof(Schemata)), "..", "test/schemata/row_constraints.yaml")
    schema   = readschema(filename)
    indata   = DataFrame(
                  patientid = UInt.([1,2,3]),
                  dob=Date.(["1992-10-01", "1988-03-23", "1983-11-18"]),
                  date_of_marriage=[Date("2015-09-13"), missing, Date("1981-11-01")])
    CSV.write(data_infile, indata; delim=',')
    compare(schema.tables[:dates], data_infile; output_data_file=data_outfile, input_issues_file=issues_infile, output_issues_file=issues_outfile)
end
test_row_constraints()
issues_in = DataFrame(CSV.File(issues_infile))
@test size(issues_in, 1) == 1

################################################################################
# Clean up

rm(data_infile)
rm(data_outfile)
rm(issues_infile)
rm(issues_outfile)
