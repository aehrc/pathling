#General conventions
- create a separate test class for each category of functions (e.g. for Existence functions  `ExistenceFunctionsDslTest`)
- create a separate method for each fhirpath function in the category (e.g. `testCount`, `testSum`, `testAllTrue`, etc.)
- in any testing expression only use implemented fhirpath functions and operators defined in `fhirpath/spec/supported.md`
- for any fhirpath type used in testing use at least an empty value of the type, a single value of the type and an array of the type
- whenever a function supports any collection type, create test cases at least for String, Boolean, Integer, Quantity and a complex types and the empty literal '{}'
