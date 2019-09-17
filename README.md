# Schemata.jl

A `Schema` is a specification of a data set.

It exists independently of any particular data set, and therefore can be constructed and modified in the absence of a data set.

This package facilitates 3 use cases:

1. Read/write a schema from/to a yaml file. Thus schemata are portable, and a change to a schema does not require recompilation.

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


# TODO

1. Implement `writeschema` (requires a write function to be implemented in `YAML.jl`).

2. Define joins between tables within a schema, which induce `intrarow_constraints` across tables.

3. Infer a `Schema` from a given data table.

4. Replace the dependence on DataFrames with dependence on the `Tables` interface.
