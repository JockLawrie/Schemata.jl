# Schemata.jl


A `Schema` is a specification of a data set.

It exists independently of any particular data set. That is, it can be constructed and modified in the absence of a data set.

This package supports 2 main operations:

1. A data set can be compared to a schema and a list of non-compliance issues returned.

2. A data set can be compared to a schema, modified where possible to comply with the schema, and a list of remaining non-compliance issues returned.


# Usage


```julia
using DataFrames
using Schemata

### Example 1

# Schema
patientid = ColumnSchema(:patientid, "Patient ID",  UInt,   !CATEGORICAL, IS_REQUIRED,  IS_UNIQUE, UInt)
age       = ColumnSchema(:age,       "Age (years)", Int,    !CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, Int)
dose      = ColumnSchema(:dose,      "Dose size",   String,  CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, ["small", "medium", "large"])
fever     = ColumnSchema(:fever,     "Had fever",   Bool,    CATEGORICAL, IS_REQUIRED, !IS_UNIQUE, Bool)
ts        = TableSchema(:mytable, "My table", [patientid, age, dose, fever], [:patientid])
schema    = Schema(:myschema, [ts])

# Data
tbl = DataFrame(
    patientid = [1, 2, 3, 4],
    age       = [11, 22, 33, 444],
    dose      = ["small", "medium", "large", "medium"],
    fever     = [false, true, true, false]
)

# Compare data to schema
diagnose(tbl, schema.tables[:mytable])

# Modify data to comply with the schema
pool!(tbl, [:dose, :fever])                                  # Ensure :dose and :fever contain categorical data
tbl[:patientid] = convert(DataArray{UInt}, tbl[:patientid])  # Change data type

# Compare again
diagnose(tbl, schema.tables[:mytable])

# Modify schema: Require :age <= 120
schema.tables[:mytable].columns[:age].valid_values = 0:120

# Compare again
diagnose(tbl, schema.tables[:mytable])

# Fix remaining data issue
tbl[4, :age] = 44  # Fix data entry error
diagnose(tbl, schema.tables[:mytable])


### Example 2
tbl[4, :age] = 9999
diagnose(tbl, schema)  # 1 issue
tbl, issues = enforce_schema(tbl, schema; exclude_invalid_values=true);  # Set invalid values to null
tbl     # Non-compliant value was set to null
issues  # No issues remaining


### Example 3
schema = xxx  # Add zip code
tbl[4, :age] = 9999
tbl[:zip]    = ["11111", "22222", "33333", "NULL"];  # CSV file was supplied with "NULL" values, forcing eltype to equal String.
tbl
diagnose(tbl, schema)  # 2 issues (tbl[4, :age] and typeof(tbl[:zip]))
tbl2, issues = enforce_schema(tbl, schema; exclude_invalid_values=true);
tbl2
issues  # 1 issue remaining (tbl[4, :zip] can't be parsed as Int)
tbl[4, :zip] = null # issue fixed
issues = enforce_schema!(tbl2, tbl, schema; exclude_invalid_values=true)  # No issues remaining
tbl2
```


# TODO

1. Handle Dates.

2. Read in a `Schema` from a YAML file.

3. Implement `intrarow_constraints` for `TableSchema`.

4. Define joins between tables within a schema, as well as intrarow_constraints across tables.
