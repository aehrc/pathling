## 1. Java null stripping

- [x] 1.1 Add test cases to `SingleInstanceEvaluatorTest` for null-valued field
      stripping in `sanitiseRow`, including nested structs
- [x] 1.2 Modify `sanitiseRow` in `SingleInstanceEvaluator` to skip fields with
      null values alongside synthetic fields
- [x] 1.3 Run fhirpath module tests and verify they pass

## 2. Python cleanup

- [x] 2.1 Remove `_strip_nulls` function from `parameters.py` and simplify
      `_build_result_part` to treat complex type values as opaque strings
- [x] 2.2 Remove the dict-based null-stripping tests from `test_parameters.py`
- [x] 2.3 Run fhirpath-lab-api tests and verify they pass
