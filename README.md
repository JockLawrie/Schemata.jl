# Schema.jl


A `Schema` is a specification of a data set.

It exists independently of any particular data set. That is, it can be constructed and modified in the absence of a data set.

A data set can be compared to a schema and a list of non-compliance issues returned.

A data set can be compared to a schema, modified where possible to comply with the schema, and a list of remaining non-compliance issues returned.


# Basic Usage


```julia
using DataFrames
using Schema

### Example 1
schema = xxx  # Schema defined before data is seen
tbl = DataFrame(age = [11, 22, 33, 444], dose = ["small", "medium", "large", "medium"], fever = ["no", "yes", "yes", "no"])
diagnose!(tbl, schema)  # No issues
# Change schema: Forbid tbl[:age] having values of 120 or above
diagnose!(tbl, schema)  # 1 issue: Probably a data entry error
tbl[4, :age] = 44       # Fix data entry error
diagnose!(tbl, schema)  # No issues


### Example 2
tbl[4, :age] = 9999
diagnose!(tbl, schema)  # 1 issue
tbl, issues = enforce_schema(tbl, schema; exclude_invalid_values=true);  # Set invalid values to null
tbl     # Non-compliant value was set to null
issues  # No issues remaining


### Example 3
schema = xxx  # Add zip code
tbl[4, :age] = 9999
tbl[:zip]    = ["11111", "22222", "33333", "NULL"];  # CSV file was supplied with "NULL" values, forcing eltype to equal String.
tbl
diagnose!(tbl, schema)  # 2 issues (tbl[4, :age] and typeof(tbl[:zip]))
tbl2, issues = enforce_schema(tbl, schema; exclude_invalid_values=true);
tbl2
issues  # 1 issue remaining (tbl[4, :zip] can't be parsed as Int)
tbl[4, :zip] = null # issue fixed
issues = enforce_schema!(tbl2, tbl, schema; exclude_invalid_values=true)  # No issues remaining
tbl2


### Example 4
primary_key constraint


### Example 5
intrarow_constraint...people under 18 can't have a large dose
```
