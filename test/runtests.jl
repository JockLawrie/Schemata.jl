using Base.Test
using Schemata
using DataFrames


################################################################################
### Test constructors 
cs = ColumnSchema(:customer_id, "Customer ID", Int, !CATEGORICAL, IS_REQUIRED, IS_UNIQUE, 1:1_000_000)
ts = TableSchema(:mytable, "My table", [cs], [:customer_id])
schema = Schema(:myschema, "My data set", [ts])

@test_throws ErrorException ColumnSchema(:customer_id, "Customer ID", Int, !CATEGORICAL, IS_REQUIRED, IS_UNIQUE, UInt)     # Int != UInt
@test_throws ErrorException ColumnSchema(:new, "Customer is new", Char, CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, ["y", "n"])  # Char != String
@test_throws ErrorException TableSchema(:mytable, "My table", [cs], [:XXXcustomer_id])  # primary_key non-existent


################################################################################
### Compare DataFrame to Schema

# Schema
patientid = ColumnSchema(:patientid, "Patient ID",  UInt,   !CATEGORICAL, IS_REQUIRED,  IS_UNIQUE, UInt)
age       = ColumnSchema(:age,       "Age (years)", Int,    !CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, Int)
dose      = ColumnSchema(:dose,      "Dose size",   String,  CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, ["small", "medium", "large"])
fever     = ColumnSchema(:fever,     "Had fever",   Bool,    CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, Bool)
ts        = TableSchema(:mytable, "My table", [patientid, age, dose, fever], [:patientid])
schema    = Schema(:fever, "Fever schema", [ts])

pid2 = ColumnSchema(:pid2, "Patient ID", UInt, !CATEGORICAL, !IS_REQUIRED, IS_UNIQUE, UInt)
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
pool!(tbl, [:dose, :fever])  # Ensure :dose and :fever contain categorical data
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 1

tbl[:patientid] = convert(DataArray{UInt}, tbl[:patientid])
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 0

# Modify schema: Forbid tbl[:age] having values of 120 or above
schema.tables[:mytable].columns[:age].valid_values = 0:120

# Compare again
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 1

tbl[4, :age] = 44       # Fix data entry error
issues = diagnose(tbl, schema.tables[:mytable])
@test size(issues, 1) == 0
