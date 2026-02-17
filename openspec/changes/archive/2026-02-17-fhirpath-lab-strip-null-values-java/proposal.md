## Why

The FHIRPath Lab API returns complex type results (e.g., Quantity, HumanName)
with null-valued fields included in the JSON output. This produces noisy
responses like `{"id":null,"value":1.5,"comparator":null,...}` instead of the
clean `{"value":1.5,...}` that clients expect. The null stripping logic added in
the Python layer is unreachable because complex types arrive as pre-serialised
JSON strings from the Java evaluator.

## What changes

- Modify `SingleInstanceEvaluator.sanitiseRow()` in the fhirpath module to
  strip null-valued fields alongside synthetic fields, so the JSON produced by
  `rowToJson()` is clean at the source.
- Remove the now-unnecessary `_strip_nulls` function and associated logic from
  the Python `parameters.py` module.

## Capabilities

### New capabilities

None.

### Modified capabilities

- `fhirpath-lab-server`: The null stripping requirement is already specified but
  the implementation location changes from Python to Java.

## Impact

- `fhirpath/src/main/java/au/csiro/pathling/fhirpath/evaluation/SingleInstanceEvaluator.java`:
  `sanitiseRow()` gains null-value filtering.
- `fhirpath/src/test/java/au/csiro/pathling/fhirpath/evaluation/SingleInstanceEvaluatorTest.java`:
  Tests updated to verify null stripping.
- `fhirpath-lab-api/src/fhirpath_lab_api/parameters.py`: `_strip_nulls` removed,
  `_build_result_part` simplified.
- `fhirpath-lab-api/tests/test_parameters.py`: Null-stripping tests removed or
  updated.
