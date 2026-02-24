## 1. Update tests

- [x] 1.1 Update all test URLs in `fhirpath-lab-api/tests/test_app.py` from `/$fhirpath-r4` to `/fhir/$fhirpath`
- [x] 1.2 Run tests and verify they fail against the old route

## 2. Update route

- [x] 2.1 Change the `@app.route` decorator in `fhirpath-lab-api/src/fhirpath_lab_api/app.py` from `/$fhirpath-r4` to `/fhir/$fhirpath`
- [x] 2.2 Run tests and verify they pass

## 3. Update documentation and specs

- [x] 3.1 Update `fhirpath-lab-api/README.md` to reference the new endpoint path
- [x] 3.2 Sync delta spec to main spec via `openspec sync`
