# Schemata.jl

A `Schema` is a specification of a data set.

It exists independently of any particular data set, and therefore can be constructed and modified in the absence of a data set.

This package facilitates 3 use cases:

1. Read/write a schema from/to a yaml file. Thus schemata are portable, and a change to a schema does not require recompilation.

2. Compare a data set to a schema and list the non-compliance issues.

3. Transform an existing data set in order to comply with a schema as much as possible (then rerun the compare function to see any outstanding issues).


# Usage

```julia
using DataFrames
using Schemata

# Read in the schema
schema = readschema(joinpath(dirname(pathof(Schemata)), "..", "test/schemata/fever.yaml"))

# Or construct the schema within the code
patientid = ColumnSchema(:patientid, "Patient ID",  UInt,   !CATEGORICAL, IS_REQUIRED,  IS_UNIQUE, UInt)
age       = ColumnSchema(:age,       "Age (years)", Int,    !CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, Int)
dose      = ColumnSchema(:dose,      "Dose size",   String,  CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, ["small", "medium", "large"])
fever     = ColumnSchema(:fever,     "Had fever",   Bool,    CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, Bool)
ts        = TableSchema(:mytable, "My table", [patientid, age, dose, fever], [:patientid])
schema    = Schema(:fever, "Fever schema", Dict(:mytable => ts))

# Data
tbl = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],
    dose      = ["small", "medium", "large", "medium"],
    fever     = [false, true, true, false]
)

# Compare the data to the schema
diagnose(tbl, schema.tables[:mytable])

# Modify the data to comply with the schema
pool!(tbl, [:dose, :fever])                                  # Ensure :dose and :fever contain categorical data
tbl[:patientid] = convert(DataArray{UInt}, tbl[:patientid])  # Change data type

# Compare again
diagnose(tbl, schema.tables[:mytable])

# Modify the schema: Require :age <= 120
schema.tables[:mytable].columns[:age].valid_values = 0:120

# Compare again
diagnose(tbl, schema.tables[:mytable])  # Looks like a data entry error

# Fix the data: Attempt 1 (do not set invalid values to NA)
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], false);
tbl
issues

# Fix the data: Attempt 2 (set invalid values to NA)
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
tbl
issues

# Fix the data: Attempt 3 (manually fix data entry error)
tbl[4, :age] = 44
diagnose(tbl, schema.tables[:mytable])

# Add a new column to the schema
zipcode = ColumnSchema(:zipcode, "Zip code", Int, CATEGORICAL, !IS_REQUIRED, !IS_UNIQUE, 10000:99999)
insert_column!(schema.tables[:mytable], zipcode)

# Write the updated schema to disk
# TODO: writeschema(joinpath(dirname(pathof(Schemata)), "..", "test/schemata/fever_updated.yaml"), schema)

# Add a corresponding (non-compliant) column to the data
tbl[:zipcode] = ["11111", "22222", "33333", "NULL"];  # CSV file was supplied with "NULL" values, forcing eltype to be String.
diagnose(tbl, schema.tables[:mytable])

# Fix the data
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
tbl
issues

# Add a Date column to the schema; note the args in the datatype
datatype = Dict("type" => Date, "args" => "Y-m-d")
dosedate = ColumnSchema(:date, "Dose date", datatype, CATEGORICAL, !IS_REQUIRED, !IS_UNIQUE, datatype)
insert_column!(schema.tables[:mytable], dosedate)

# Add a corresponding (compliant) column to the data
tbl[:date] = ["2017-12-01", "2017-12-01", "2017-12-11", "2017-12-09"];
diagnose(tbl, schema.tables[:mytable])
tbl, issues = enforce_schema(tbl, schema.tables[:mytable], true);
tbl
issues
```


# TODO

1. Remove hard coded Date handling.

2. Implement writeschema.

3. Implement `intrarow_constraints` for `TableSchema`.

4. Define joins between tables within a schema, as well as intrarow_constraints across tables.

5. Infer a simple `Schema` from a given data table.

6. Remove dependence on DataFrames?
