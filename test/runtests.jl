using Base.Test
using Schemata
using DataFrames


################################################################################
### Basic schema
cs = ColumnSchema(:customer_id, "Customer ID", Int, !CATEGORICAL, !CATS_ORDERED, IS_REQUIRED, IS_UNIQUE, 1:1_000_000)
ts = TableSchema(:mytable, "My table", [cs], [:customer_id], Function[], Function[])
schema = Schema(:myschema, [ts])

@test_throws ErrorException ColumnSchema(:customer_id, "Customer ID", Int, false, false, true, true, UInt)      # Int != UInt
@test_throws ErrorException TableSchema(:mytable, "My table", [cs], [:XXXcustomer_id], Function[], Function[])  # primary_key non-existent


################################################################################
### Compare DataFrame to Schema

# Schema
patientid = ColumnSchema(:patientid, "Patient ID",  UInt,   !CATEGORICAL, !CATS_ORDERED, IS_REQUIRED,  IS_UNIQUE, UInt)
age       = ColumnSchema(:age,       "Age (years)", UInt,   !CATEGORICAL, !CATS_ORDERED, IS_REQUIRED, !IS_UNIQUE, UInt)
gender    = ColumnSchema(:gender,    "Gender",      Char,    CATEGORICAL, !CATS_ORDERED, IS_REQUIRED, !IS_UNIQUE, Set(['m', 'f']))
dose      = ColumnSchema(:dose,      "Dose size",   String,  CATEGORICAL,  CATS_ORDERED, IS_REQUIRED, !IS_UNIQUE, String)
fever     = ColumnSchema(:fever,     "Had fever",   Bool,    CATEGORICAL, !CATS_ORDERED, IS_REQUIRED, !IS_UNIQUE, Bool)
ts     = TableSchema(:mytable, "My table", [patientid, age, gender, dose, fever], [:patientid], Function[], Function[])
schema = Schema(:myschema, [ts])

@test_throws ErrorException ColumnSchema(:gender, "Gender", Char, CATEGORICAL, !CATS_ORDERED, IS_REQUIRED, !IS_UNIQUE, Set(["m", "f"]))  # Char != String
pid2 = ColumnSchema(:patientid, "Patient ID", UInt, !CATEGORICAL, !CATS_ORDERED, !IS_REQUIRED, IS_UNIQUE, UInt)
@test_throws ErrorException TableSchema(:mytable, "My table", [patientid, age, gender, dose, fever], [:pid2], Function[], Function[])

# DataFrame
tbl = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],
    gender    = ['f', 'm', 'f', 'm'],
    dose      = ["small", "medium", "large", "medium"],
    fever     = ["no", "yes", "yes", "no"]
)

#=
# Compare
diagnose!(tbl, schema)  # No issues
# Change schema: Forbid tbl[:age] having values of 120 or above
diagnose!(tbl, schema)  # 1 issue: Probably a data entry error
tbl[4, :age] = 44       # Fix data entry error
diagnose!(tbl, schema)  # No issues
=#
