## 1. Request model

- [x] 1.1 Add `typeFilters` field (`Map<String, List<String>>`) to `ExportRequest` record, keyed by resource type code with search query strings as values

## 2. Parsing and validation

- [x] 2.1 Write unit tests for `_typeFilter` parsing and validation in `ExportOperationValidatorTest` (format validation, resource type validation, search parameter validation, strict/lenient handling, consistency with `_type`, implicit type inclusion)
- [x] 2.2 Implement `_typeFilter` parsing in `ExportOperationValidator` (split on `?`, extract resource type and search query string, validate resource type)
- [x] 2.3 Remove `_typeFilter` from `UNSUPPORTED_QUERY_PARAMS` set
- [x] 2.4 Implement implicit resource type inclusion when `_type` is absent but `_typeFilter` is present
- [x] 2.5 Implement consistency validation between `_type` and `_typeFilter` (strict/lenient)

## 3. Export providers

- [x] 3.1 Add `_typeFilter` `@OperationParam` to `SystemExportProvider.export()` and update `preAsyncValidate()`
- [x] 3.2 Add `_typeFilter` `@OperationParam` to `PatientExportProvider.exportAllPatients()` and `exportSinglePatient()` and update `preAsyncValidate()`
- [x] 3.3 Add `_typeFilter` `@OperationParam` to `GroupExportProvider.exportGroup()` and update `preAsyncValidate()`
- [x] 3.4 Pass `_typeFilter` through validator methods (`validateRequest` and `validatePatientExportRequest`)

## 4. Export execution

- [x] 4.1 Write unit tests for `_typeFilter` application in `ExportExecutor` (single filter, multiple filters OR logic, no matching resources, filter for type not in export)
- [x] 4.2 Implement `applyTypeFilters` method in `ExportExecutor` using `PathlingContext.searchToColumn()` to generate Spark filter columns
- [x] 4.3 Integrate `applyTypeFilters` into the `execute()` pipeline (after date filtering, before patient compartment filtering)

## 5. Integration tests

- [x] 5.1 Add integration tests for `_typeFilter` on system-level export (valid filter, filter reduces output, multiple filters OR logic)
- [x] 5.2 Add integration tests for `_typeFilter` on patient-level export
- [x] 5.3 Add integration tests for `_typeFilter` strict/lenient error handling (invalid format, unknown resource type, type not in `_type`, unknown search parameter)
- [x] 5.4 Add integration test for `_typeFilter` without `_type` (implicit type inclusion)
