---
sidebar_position: 7
description: The view run operation executes a ViewDefinition against FHIR data and returns tabular results in NDJSON or CSV format.
---

# Run view

This operation executes a
[ViewDefinition](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html)
against FHIR data and returns the results as tabular data. This implementation
follows the
[SQL on FHIR specification](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-ViewDefinitionRun.html).

## Endpoint

The operation is available at the system, type, and instance levels:

```
POST [base]/$viewdefinition-run
POST [base]/ViewDefinition/$run
GET|POST [base]/ViewDefinition/[id]/$run
```

At the system and type levels, supply exactly one of `viewResource` (inline) or
`viewReference` (a reference to a stored ViewDefinition). At the instance level,
the view is taken from the path.

## Parameters

| Name            | Cardinality | Type      | Description                                                                                                                                    |
| --------------- | ----------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `viewResource`  | 0..1        | Resource  | An inline ViewDefinition resource. Mutually exclusive with `viewReference`.                                                                    |
| `viewReference` | 0..1        | Reference | A reference to a stored ViewDefinition (e.g. `ViewDefinition/patient-demographics`). Mutually exclusive with `viewResource`.                   |
| `_format`       | 0..1        | code      | Output format: `json`, `ndjson` (default), or `csv`, or the corresponding media type. An unsupported value returns `400`.                      |
| `header`        | 0..1        | boolean   | Include header row in CSV output. Defaults to `true`.                                                                                          |
| `_limit`        | 0..1        | integer   | Maximum number of rows to return.                                                                                                              |
| `patient`       | 0..1        | Reference | Filter to resources for the specified patient. More than one value returns `400`.                                                              |
| `group`         | 0..\*       | Reference | Filter to resources for patients in the specified Group(s).                                                                                    |
| `_since`        | 0..1        | instant   | Only include resources where `meta.lastUpdated` is at or after this time.                                                                      |
| `resource`      | 0..\*       | Resource  | Inline FHIR resources. A `Bundle` value is expanded to its entry resources. When provided, these are used instead of the server's stored data. |

The `source` parameter (an external data source) is not supported by this
server; supplying it returns `400 Bad Request` with an OperationOutcome.

## Status codes

| Status                     | Condition                                                                                                                                                                                                  |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `200 OK`                   | Success. The body is the rows in the negotiated format.                                                                                                                                                    |
| `400 Bad Request`          | Malformed request or invalid parameter: an unsupported `_format`, the unsupported `source` parameter, both `viewResource` and `viewReference`, neither supplied (system/type), or more than one `patient`. |
| `404 Not Found`            | A `viewReference` (or instance `[id]`) that does not resolve to a stored ViewDefinition.                                                                                                                   |
| `422 Unprocessable Entity` | A well-formed request carrying a semantically invalid ViewDefinition.                                                                                                                                      |

When no `_format` is supplied, the format is negotiated from the `Accept` header,
falling back to NDJSON when nothing matches.

## Request format

The operation accepts a FHIR Parameters resource with the ViewDefinition passed
as a nested resource:

```http
POST [base]/$viewdefinition-run HTTP/1.1
Content-Type: application/fhir+json
Accept: application/x-ndjson

{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "viewResource",
            "resource": {
                "resourceType": "ViewDefinition",
                "name": "patient_demographics",
                "resource": "Patient",
                "status": "active",
                "select": [
                    {
                        "column": [
                            {
                                "name": "id",
                                "path": "id"
                            }
                        ]
                    },
                    {
                        "column": [
                            {
                                "name": "family",
                                "path": "name.first().family"
                            }
                        ]
                    },
                    {
                        "column": [
                            {
                                "name": "given",
                                "path": "name.first().given.first()"
                            }
                        ]
                    }
                ]
            }
        }
    ]
}
```

## Running a stored ViewDefinition by reference

Instead of sending the ViewDefinition inline, you can run one that is already
stored on the server by supplying a `viewReference`:

```http
POST [base]/ViewDefinition/$run HTTP/1.1
Content-Type: application/fhir+json
Accept: application/x-ndjson

{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "viewReference",
            "valueReference": {
                "reference": "ViewDefinition/patient-demographics"
            }
        }
    ]
}
```

The reference must be a literal relative reference of the form
`ViewDefinition/[id]`. A reference that does not resolve returns `404 Not Found`.

## Response formats

The response uses HTTP chunked transfer encoding, allowing clients to process
results incrementally as they arrive rather than waiting for the complete
response. This is particularly useful for large result sets.

### NDJSON

When `_format` is `application/x-ndjson` (the default), the response is
newline-delimited JSON with one row per line:

```
Content-Type: application/x-ndjson

{"id":"patient-1","family":"Smith","given":"John"}
{"id":"patient-2","family":"Jones","given":"Jane"}
```

### CSV

When `_format` is `text/csv`, the response is comma-separated values:

```
Content-Type: text/csv

id,family,given
patient-1,Smith,John
patient-2,Jones,Jane
```

Set `header=false` to exclude the header row.

## ViewDefinition structure

A ViewDefinition specifies which resource type to query and how to extract
columns. Here is a more readable example:

```json
{
    "resourceType": "ViewDefinition",
    "name": "patient_demographics",
    "resource": "Patient",
    "status": "active",
    "select": [
        {
            "column": [
                {
                    "name": "id",
                    "path": "id"
                }
            ]
        },
        {
            "column": [
                {
                    "name": "family",
                    "path": "name.first().family"
                }
            ]
        },
        {
            "column": [
                {
                    "name": "given",
                    "path": "name.first().given.first()"
                }
            ]
        },
        {
            "column": [
                {
                    "name": "gender",
                    "path": "gender"
                }
            ]
        },
        {
            "column": [
                {
                    "name": "birth_date",
                    "path": "birthDate"
                }
            ]
        }
    ]
}
```

The `path` values use [FHIRPath](https://hl7.org/fhirpath/) expressions to
extract data from resources.

## Inline resources

The `resource` parameter allows you to execute a ViewDefinition against
provided FHIR resources instead of the server's stored data. This is useful for
testing ViewDefinitions or processing resources without importing them.

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "viewResource",
            "resource": {
                "resourceType": "ViewDefinition",
                "...": "..."
            }
        },
        {
            "name": "resource",
            "valueString": "{\"resourceType\":\"Patient\",\"id\":\"test-1\",\"name\":[{\"family\":\"Smith\"}]}"
        },
        {
            "name": "resource",
            "valueString": "{\"resourceType\":\"Patient\",\"id\":\"test-2\",\"name\":[{\"family\":\"Jones\"}]}"
        }
    ]
}
```

## Filtering

### Patient filter

Use the `patient` parameter (a single `Reference`) to restrict results to
resources associated with that patient:

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "viewResource",
            "resource": {
                "resourceType": "ViewDefinition",
                "...": "..."
            }
        },
        {
            "name": "patient",
            "valueReference": {
                "reference": "Patient/patient-123"
            }
        }
    ]
}
```

### Group filter

Use the `group` parameter to restrict results to resources for patients who are
members of the specified Group(s). It accepts one or more `Reference` values:

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "viewResource",
            "resource": {
                "resourceType": "ViewDefinition",
                "...": "..."
            }
        },
        {
            "name": "group",
            "valueReference": {
                "reference": "Group/cohort-study-group"
            }
        }
    ]
}
```

### Time filter

Use `_since` to only include resources updated after a specific time:

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "viewResource",
            "resource": {
                "resourceType": "ViewDefinition",
                "...": "..."
            }
        },
        {
            "name": "_since",
            "valueInstant": "2024-01-01T00:00:00Z"
        }
    ]
}
```

## Python example

The following Python script demonstrates the view run operation.

Run the script using [uv](https://docs.astral.sh/uv/):

```bash
uv run view_run_client.py
```

### View run client

```python
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///
"""Demonstrates the $viewdefinition-run operation."""

import json
import requests

BASE_URL = "https://pathling.example.com/fhir"


def create_patient_view():
    """Create a ViewDefinition for Patient demographics."""
    return {
        "resourceType": "ViewDefinition",
        "name": "patient_demographics",
        "resource": "Patient",
        "status": "active",
        "select": [
            {"column": [{"name": "id", "path": "id"}]},
            {"column": [{"name": "family", "path": "name.first().family"}]},
            {"column": [
                {"name": "given", "path": "name.first().given.first()"}]},
            {"column": [{"name": "gender", "path": "gender"}]},
            {"column": [{"name": "birth_date", "path": "birthDate"}]},
        ],
    }


def run_view(view_definition, output_format="ndjson", limit=None):
    """Execute a ViewDefinition and return the results."""
    url = f"{BASE_URL}/$viewdefinition-run"

    # Build FHIR Parameters resource.
    parameters = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "viewResource", "resource": view_definition}
        ],
    }

    if output_format == "csv":
        parameters["parameter"].append(
            {"name": "_format", "valueString": "text/csv"}
        )
    else:
        parameters["parameter"].append(
            {"name": "_format", "valueString": "application/x-ndjson"}
        )

    if limit:
        parameters["parameter"].append(
            {"name": "_limit", "valueInteger": limit}
        )

    # Set Accept header based on format.
    accept = "text/csv" if output_format == "csv" else "application/x-ndjson"

    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": accept,
    }

    response = requests.post(url, json=parameters, headers=headers, stream=True)
    response.raise_for_status()

    return response


def run_view_with_inline_resources(view_definition, resources):
    """Execute a ViewDefinition against inline resources."""
    url = f"{BASE_URL}/$viewdefinition-run"

    parameters = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "viewResource", "resource": view_definition},
            {"name": "_format", "valueString": "application/x-ndjson"},
        ],
    }

    # Add inline resources.
    for resource in resources:
        parameters["parameter"].append(
            {"name": "resource", "valueString": json.dumps(resource)}
        )

    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": "application/x-ndjson",
    }

    response = requests.post(url, json=parameters, headers=headers, stream=True)
    response.raise_for_status()

    return response


def main():
    """Execute the view run operation."""
    view = create_patient_view()

    print("Running ViewDefinition against server data...")
    print("-" * 50)

    response = run_view(view, output_format="ndjson", limit=10)

    for line in response.iter_lines(decode_unicode=True):
        if line:
            row = json.loads(line)
            print(
                f"Patient: {row.get('id')} - {row.get('given')} {row.get('family')}")

    print("-" * 50)
    print("\nRunning ViewDefinition with inline resources...")
    print("-" * 50)

    # Test with inline resources.
    test_patients = [
        {
            "resourceType": "Patient",
            "id": "inline-1",
            "name": [{"family": "Smith", "given": ["John"]}],
            "gender": "male",
            "birthDate": "1980-01-15",
        },
        {
            "resourceType": "Patient",
            "id": "inline-2",
            "name": [{"family": "Jones", "given": ["Jane"]}],
            "gender": "female",
            "birthDate": "1990-06-20",
        },
    ]

    response = run_view_with_inline_resources(view, test_patients)

    for line in response.iter_lines(decode_unicode=True):
        if line:
            row = json.loads(line)
            print(
                f"Patient: {row.get('id')} - {row.get('given')} {row.get('family')}")

    print("-" * 50)
    print("Done!")


if __name__ == "__main__":
    main()
```
