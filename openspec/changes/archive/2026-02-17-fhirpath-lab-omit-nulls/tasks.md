## 1. Tests

- [x] 1.1 Add test for null stripping in complex type result serialisation
- [x] 1.2 Add test for recursive null stripping in nested structs

## 2. Implementation

- [x] 2.1 Add recursive null-stripping helper to `parameters.py`
- [x] 2.2 Apply helper in `_build_result_part` before `json.dumps`

## 3. Verification

- [x] 3.1 Run full test suite for `fhirpath-lab-api`
