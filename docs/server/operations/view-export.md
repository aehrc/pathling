# Export view

This operation executes one or more [ViewDefinitions](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html) against FHIR data and exports the results to files. This implementation follows the [SQL on FHIR specification](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/OperationDefinition-ViewDefinitionExport.html).

Use this operation for large datasets that need to be exported asynchronously. For smaller, synchronous queries, see the [run view](/docs/server/operations/view-run.md) operation.

## Endpoint[​](#endpoint "Direct link to Endpoint")

```
POST [base]/$viewdefinition-export
```

## Parameters[​](#parameters "Direct link to Parameters")

| Name                 | Cardinality | Type      | Description                                                                                                                  |
| -------------------- | ----------- | --------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `view`               | 1..\*       | (parts)   | A repeating parameter; each repetition identifies one ViewDefinition to export.                                              |
| `view.name`          | 0..1        | string    | Optional name for the exported view. Used in output filenames.                                                               |
| `view.viewResource`  | 0..1        | Resource  | An inline ViewDefinition resource. Mutually exclusive with `view.viewReference`.                                             |
| `view.viewReference` | 0..1        | Reference | A reference to a stored ViewDefinition. Mutually exclusive with `view.viewResource`.                                         |
| `clientTrackingId`   | 0..1        | string    | Client-provided tracking identifier; echoed in the completion manifest.                                                      |
| `_format`            | 0..1        | code      | Output format: `ndjson` (default), `csv`, or `parquet`, or the corresponding media type. An unsupported value returns `400`. |
| `header`             | 0..1        | boolean   | Include header row in CSV output. Defaults to `true`.                                                                        |
| `patient`            | 0..\*       | id        | Filter to resources for the specified patient(s).                                                                            |
| `group`              | 0..\*       | id        | Filter to resources for patients in the specified Group(s).                                                                  |
| `_since`             | 0..1        | instant   | Only include resources where `meta.lastUpdated` is at or after this time.                                                    |

Each `view` part must supply exactly one of `view.viewResource` or `view.viewReference`; supplying both, or neither, returns `400 Bad Request`. A `view.viewReference` that does not resolve to a stored ViewDefinition returns `404 Not Found`. These rejections are returned synchronously at kick-off.

The `source` parameter (an external data source) is not supported by this server; supplying it returns `400 Bad Request` with an OperationOutcome, synchronously at kick-off.

## Asynchronous processing[​](#asynchronous-processing "Direct link to Asynchronous processing")

The view export operation uses the [FHIR Asynchronous Request Pattern](https://hl7.org/fhir/R4/async.html). Include a `Prefer: respond-async` header to initiate asynchronous processing.

<!-- -->

### Kick-off request[​](#kick-off-request "Direct link to Kick-off request")

```
POST [base]/$viewdefinition-export HTTP/1.1
Content-Type: application/fhir+json
Accept: application/fhir+json
Prefer: respond-async

{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "view",
            "part": [
                {
                    "name": "name",
                    "valueString": "patient_demographics"
                },
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
                                    {"name": "id", "path": "id"},
                                    {"name": "family", "path": "name.first().family"},
                                    {"name": "given", "path": "name.first().given.first()"},
                                    {"name": "gender", "path": "gender"},
                                    {"name": "birth_date", "path": "birthDate"}
                                ]
                            }
                        ]
                    }
                }
            ]
        }
    ]
}
```

### Kick-off response[​](#kick-off-response "Direct link to Kick-off response")

The `202 Accepted` response carries a `Parameters` acknowledgement with the export status and identifier, and a `Content-Location` header pointing at the status URL:

```
HTTP/1.1 202 Accepted
Content-Location: [base]/$job?id=[job-id]

{
    "resourceType": "Parameters",
    "parameter": [
        {"name": "status", "valueCode": "accepted"},
        {"name": "exportId", "valueString": "[job-id]"}
    ]
}
```

### Polling[​](#polling "Direct link to Polling")

Poll the URL from `Content-Location` until you receive a `200 OK` response with the export manifest.

* `202 Accepted` — Export still in progress. Check `X-Progress` header for status.
* `200 OK` — Export complete. Response body contains the manifest.

## Response manifest[​](#response-manifest "Direct link to Response manifest")

When the export completes, the response is a FHIR `Parameters` resource following the SQL on FHIR shape:

```
{
    "resourceType": "Parameters",
    "parameter": [
        { "name": "exportId", "valueString": "abc123" },
        { "name": "status", "valueCode": "completed" },
        { "name": "clientTrackingId", "valueString": "my-tracking-id" },
        { "name": "_format", "valueCode": "ndjson" },
        {
            "name": "exportStartTime",
            "valueInstant": "2026-06-21T01:00:00.000Z"
        },
        { "name": "exportEndTime", "valueInstant": "2026-06-21T01:00:12.000Z" },
        { "name": "exportDuration", "valueInteger": 12 },
        {
            "name": "output",
            "part": [
                { "name": "name", "valueString": "patient_demographics" },
                {
                    "name": "location",
                    "valueUri": "https://pathling.example.com/fhir/$result?job=abc123&file=patient_demographics.ndjson"
                }
            ]
        }
    ]
}
```

### Manifest fields[​](#manifest-fields "Direct link to Manifest fields")

| Field              | Description                                                                              |
| ------------------ | ---------------------------------------------------------------------------------------- |
| `exportId`         | The server-assigned export job identifier.                                               |
| `status`           | The export status; `completed` on the success path.                                      |
| `clientTrackingId` | The tracking identifier supplied at kick-off. Present only when one was supplied.        |
| `_format`          | The effective output format.                                                             |
| `exportStartTime`  | The time the export was initiated (kick-off).                                            |
| `exportEndTime`    | The time the export completed.                                                           |
| `exportDuration`   | The whole number of seconds between the start and end times.                             |
| `output`           | One per exported view, each with a `name` part and one or more `location` download URLs. |

A view that produced several files (for example through Spark partitioning) is represented as a single `output` with one `name` and a repeating `location` part per file.

## Output formats[​](#output-formats "Direct link to Output formats")

| Format  | `_format` value | Content type                     | Description                                                    |
| ------- | --------------- | -------------------------------- | -------------------------------------------------------------- |
| NDJSON  | `ndjson`        | `application/x-ndjson`           | Newline-delimited JSON. Default format.                        |
| CSV     | `csv`           | `text/csv`                       | Comma-separated values. Use `header=false` to exclude headers. |
| Parquet | `parquet`       | `application/vnd.apache.parquet` | Apache Parquet columnar format. Efficient for large datasets.  |

Parquet cannot represent a column whose type is unresolved (Spark `NullType`, displayed as "VOID"), such as a column that resolves to an empty collection. If a view's result contains such a column and Parquet output is requested, the export job fails and the polled status carries an `OperationOutcome` that names every offending column and suggests two remediations: add an explicit cast in the view, or choose a different output format. The NDJSON and CSV formats are unaffected.

## Multiple views[​](#multiple-views "Direct link to Multiple views")

Export multiple views in a single request by including multiple `view` parameters:

```
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "view",
            "part": [
                {"name": "name", "valueString": "patients"},
                {"name": "viewResource", "resource": {"resourceType": "ViewDefinition", "..."}}
            ]
        },
        {
            "name": "view",
            "part": [
                {"name": "name", "valueString": "conditions"},
                {"name": "viewResource", "resource": {"resourceType": "ViewDefinition", "..."}}
            ]
        }
    ]
}
```

Each view produces a separate output file in the manifest.

## Filtering[​](#filtering "Direct link to Filtering")

### Patient filter[​](#patient-filter "Direct link to Patient filter")

Use the `patient` parameter to restrict results to resources associated with specific patients:

```
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "view",
            "part": [
                {"name": "viewResource", "resource": {"resourceType": "ViewDefinition", "..."}}
            ]
        },
        {"name": "patient", "valueId": "patient-123"},
        {"name": "patient", "valueId": "patient-456"}
    ]
}
```

### Group filter[​](#group-filter "Direct link to Group filter")

Use the `group` parameter to restrict results to resources for patients who are members of the specified Group:

```
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "view",
            "part": [
                {"name": "viewResource", "resource": {"resourceType": "ViewDefinition", "..."}}
            ]
        },
        {"name": "group", "valueId": "cohort-study-group"}
    ]
}
```

### Time filter[​](#time-filter "Direct link to Time filter")

Use `_since` to only include resources updated after a specific time:

```
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "view",
            "part": [
                {"name": "viewResource", "resource": {"resourceType": "ViewDefinition", "..."}}
            ]
        },
        {"name": "_since", "valueInstant": "2024-01-01T00:00:00Z"}
    ]
}
```

## Comparison with run view[​](#comparison-with-run-view "Direct link to Comparison with run view")

| Aspect         | Export view                        | Run view                       |
| -------------- | ---------------------------------- | ------------------------------ |
| Processing     | Asynchronous with polling          | Synchronous                    |
| Output         | Files (download via manifest URLs) | Streamed response              |
| Multiple views | Yes                                | No                             |
| Parquet format | Yes                                | No                             |
| Use case       | Large datasets, batch processing   | Small queries, interactive use |

## Python example[​](#python-example "Direct link to Python example")

The following Python script demonstrates the complete view export workflow.

Run the script using [uv](https://docs.astral.sh/uv/):

```
uv run view_export_client.py
```

### View export client[​](#view-export-client "Direct link to View export client")

```
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///
"""Demonstrates the $viewdefinition-export operation with async polling."""

import os
import time
import requests

BASE_URL = "https://pathling.example.com/fhir"
OUTPUT_DIR = "./view_export"


def create_patient_view():
    """Create a ViewDefinition for Patient demographics."""
    return {
        "resourceType": "ViewDefinition",
        "name": "patient_demographics",
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "column": [
                    {"name": "id", "path": "id"},
                    {"name": "family", "path": "name.first().family"},
                    {"name": "given", "path": "name.first().given.first()"},
                    {"name": "gender", "path": "gender"},
                    {"name": "birth_date", "path": "birthDate"},
                ]
            }
        ],
    }


def create_condition_view():
    """Create a ViewDefinition for Condition data."""
    return {
        "resourceType": "ViewDefinition",
        "name": "conditions",
        "resource": "Condition",
        "status": "active",
        "select": [
            {
                "column": [
                    {"name": "id", "path": "id"},
                    {"name": "patient_id", "path": "subject.getReferenceKey()"},
                    {"name": "code", "path": "code.coding.first().code"},
                    {"name": "display", "path": "code.coding.first().display"},
                    {"name": "onset", "path": "onset.ofType(dateTime)"},
                ]
            }
        ],
    }


def kick_off_export(views, output_format="ndjson"):
    """Initiate a view export."""
    url = f"{BASE_URL}/$viewdefinition-export"

    # Build Parameters resource with view definitions.
    parameters = {
        "resourceType": "Parameters",
        "parameter": [],
    }

    for view in views:
        parameters["parameter"].append({
            "name": "view",
            "part": [
                {"name": "name", "valueString": view.get("name", "unnamed")},
                {"name": "viewResource", "resource": view},
            ],
        })

    parameters["parameter"].append({
        "name": "_format",
        "valueString": output_format,
    })

    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": "application/fhir+json",
        "Prefer": "respond-async",
    }

    response = requests.post(url, json=parameters, headers=headers)

    if response.status_code == 202:
        status_url = response.headers.get("Content-Location")
        print(f"Export started, polling: {status_url}")
        return status_url
    else:
        response.raise_for_status()


def poll_status(status_url, timeout=3600):
    """Poll the status endpoint until export completes."""
    start = time.time()
    interval = 2.0

    while time.time() - start < timeout:
        response = requests.get(
            status_url,
            headers={"Accept": "application/fhir+json"},
        )

        if response.status_code == 200:
            print("Export complete")
            return response.json()
        elif response.status_code == 202:
            progress = response.headers.get("X-Progress", "unknown")
            print(f"In progress: {progress}")
            time.sleep(interval)
            interval = min(interval * 1.5, 30.0)
        else:
            response.raise_for_status()

    raise TimeoutError(f"Export timed out after {timeout} seconds")


def download_files(manifest, output_dir):
    """Download all files from the export manifest Parameters resource."""
    os.makedirs(output_dir, exist_ok=True)

    # The manifest is a FHIR Parameters resource; each output parameter has a
    # name part and one or more location parts (one per file).
    for param in manifest.get("parameter", []):
        if param.get("name") != "output":
            continue
        parts = param.get("part", [])
        name = next(
            (p.get("valueString") for p in parts if p.get("name") == "name"),
            "output",
        )
        locations = [
            p.get("valueUri") for p in parts if p.get("name") == "location"
        ]
        for index, url in enumerate(locations):
            # Extract extension from the URL or default to ndjson.
            ext = url.split(".")[-1] if "." in url.split("=")[-1] else "ndjson"
            suffix = f"-{index}" if len(locations) > 1 else ""
            filename = f"{name}{suffix}.{ext}"
            filepath = os.path.join(output_dir, filename)

            print(f"Downloading {name}...")
            response = requests.get(url)
            response.raise_for_status()

            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"  Saved to {filepath}")


def main():
    """Execute the complete view export workflow."""
    views = [
        create_patient_view(),
        create_condition_view(),
    ]

    print(f"Starting view export for {len(views)} views...")
    status_url = kick_off_export(views, output_format="ndjson")

    manifest = poll_status(status_url)
    print(f"Manifest: {manifest}")

    download_files(manifest, OUTPUT_DIR)
    print("Export complete!")


if __name__ == "__main__":
    main()
```
