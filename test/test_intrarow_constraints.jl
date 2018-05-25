using DataFrames
using Schemata

schema = readschema("schemata/row_constraints.yaml")

d = DataFrame(
              patientid = UInt.([1,2,3]),
              dob=Date.(["1992-10-01", "1988-03-23", "1983-11-18"]),
              date_of_marriage=[Date("2015-09-13"), missing, Date("1981-11-01")]
             )

issues = diagnose(d, schema.tables[:dates])
