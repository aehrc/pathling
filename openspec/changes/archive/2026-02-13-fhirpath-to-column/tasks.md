## 1. Java library API

- [x] 1.1 Write unit tests for `fhirPathToColumn` in `PathlingContextTest` covering boolean expressions, value expressions, DataFrame filter/select usage, invalid expressions, and invalid resource types
- [x] 1.2 Add `fhirPathToColumn` method to `PathlingContext` delegating to `SearchColumnBuilder.fromExpression()`
- [x] 1.3 Verify Java tests pass

## 2. Python library

- [x] 2.1 Write tests for `fhirpath_to_column` in the Python test suite covering boolean filtering, value extraction, and invalid expressions
- [x] 2.2 Add `fhirpath_to_column` method to `PathlingContext` in `context.py` with full docstring
- [x] 2.3 Verify Python tests pass

## 3. R library

- [x] 3.1 Write tests for `pc_fhirpath_to_column` in the R test suite covering boolean filtering, value extraction, and invalid expressions
- [x] 3.2 Add `pc_fhirpath_to_column` function to `context.R` with roxygen2 documentation
- [x] 3.3 Verify R tests pass

## 4. Documentation

- [x] 4.1 Update site documentation for the Java, Python, and R libraries to include `fhirPathToColumn` / `fhirpath_to_column` / `pc_fhirpath_to_column`
