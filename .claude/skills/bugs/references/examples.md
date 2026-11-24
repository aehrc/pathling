# Bug report examples

Both examples describe the same defect: issue #2913.

## Good

The stack trace and the optional block are cut here for length; a real report keeps them. Both
versions are named because the user asked for both to be run; a report for one version names only
that one.

````markdown
**Title:** search_to_column rejects Observation?value-quantity with
InvalidSearchParameterException (SampledData)

## Summary

`search_to_column` raises on the standard `value-quantity` parameter of `Observation`, so no
filter column is produced. The error is about the `SampledData` branch of the parameter's
expression, which the FHIR specification marks as not used in search.

## Environment

- Pathling 10.0.0 (PyPI; Python 3.11, PySpark 4.0.5) and current `main` (11.0.0-SNAPSHOT build)
- FHIR R4, Python library API

## How it was found

Reproduced while reviewing this issue.

## Steps to reproduce

```python
from pathling import PathlingContext

pc = PathlingContext.create(enable_terminology=False)
pc.search_to_column("Observation", "value-quantity=5.4")
```

## Actual behaviour (observed)

```
InvalidSearchParameterException: FHIR type 'SampledData' is not valid for search
parameter type 'QUANTITY'. Allowed types: [QUANTITY]
```

(full stack trace in a collapsed block)

## Expected behaviour (from the specification, not verified end to end)

The call returns a filter column. The parameter's expression is
`(Observation.value as Quantity) | (Observation.value as SampledData)`, and the spec lists
`SampledData` as "Not used in search", so such an Observation never matches and causes no error.
An Observation with `valueQuantity` 5.4 mg should match.
````

It works because:

- The title names the call and the exception, and the summary says what fails and where.
- The snippet runs as written, and the exact error is quoted.
- The versions named are the versions that were run.
- Expected behaviour cites the spec and says it was not run end to end.
- Optional fields that were not investigated say so.

## Bad

```markdown
**Title:** Quantity search broken

## Description

Searching by quantity doesn't work on Observations, it gives an error. This has been broken for
a long time and probably affects all quantity searches, and maybe dates too. I think the problem
is in SearchColumnBuilder where it validates types, so someone should just skip the bad ones.
Works in HAPI.

## Steps

1. Load some Observations
2. Search by value-quantity
3. See error

## Expected

It should work.

## Environment

Latest version, [Python version], [OS]

TODO: add stack trace
```

| Problem                                                                   | Rule it breaks                                                                                                                        |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Title "Quantity search broken"                                            | **About one failure**: the title names no call and no error.                                                                          |
| "Gives an error"                                                          | **Quotes the error**: the text is missing, so a reproduction can't be checked and the report can't be found by searching.             |
| "Broken for a long time", "all quantity searches", "maybe dates too"      | **Stays within the evidence**: claims nobody ran, stated as fact.                                                                     |
| "I think the problem is in `SearchColumnBuilder`… just skip the bad ones" | **Stays within the evidence**: an unlabelled guess about internals, plus a prescribed fix. Label it as a hypothesis, or leave it out. |
| "Works in HAPI"                                                           | **Stays within the evidence**: a comparison with no version and no request.                                                           |
| Steps: "Load some Observations / Search / See error"                      | **Reproducible as written**: no call, no value, no data.                                                                              |
| "Expected: It should work"                                                | **Every claim observed or inferred**: no spec rule and no input that should match.                                                    |
| "Latest version", `[Python version]`, `[OS]`                              | **Reproducible** (vague version) and **finished** (placeholders that would be posted as written).                                     |
| "TODO: add stack trace"                                                   | **Finished**: a drafting note in the body, for a trace that was available.                                                            |
