---
sidebar_position: 1
description: The import operation allows FHIR data to be imported into the server from external sources such as S3, HDFS, or local filesystem.
---

# Import

This operation allows FHIR R4 data to be imported into the server, making it
available for query via other operations. Links to retrieve the data are
provided rather than sending the data inline within the request itself,
allowing for large data sets to be imported efficiently.

## Supported formats

The following source formats are supported:

| Format  | MIME type                              | Description                                                                                                  |
|---------|----------------------------------------|--------------------------------------------------------------------------------------------------------------|
| NDJSON  | `application/fhir+ndjson`              | [FHIR Newline Delimited JSON](https://hl7.org/fhir/R4/nd-json.html) format                                   |
| Parquet | `application/x-pathling-parquet`       | [Apache Parquet](https://parquet.apache.org/) conforming to the [Pathling schema](../../libraries/io/schema) |
| Delta   | `application/x-pathling-delta+parquet` | [Delta Lake](https://delta.io/) conforming to the [Pathling schema](../../libraries/io/schema)               |

## Supported URL schemes

Currently Pathling supports retrieval of files from the following URL schemes:

- [Amazon S3](https://aws.amazon.com/s3/) (`s3a://`)
- [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (`hdfs://`)
- Local filesystem (`file://`)

Authentication is supported for S3. Import sources must be explicitly
whitelisted within the configuration for security reasons.

## Asynchronous processing

The import operation requires
the [Asynchronous Request Pattern](https://hl7.org/fhir/R4/async.html). Add a
`Prefer: respond-async` header to your request to initiate asynchronous
processing. A `202 Accepted` response will be returned with a `Content-Location`
header indicating the URL of the status endpoint.

Poll the status endpoint to check progress. When the job is complete, the
response will include a `Location` header pointing to the final result.

```
POST [FHIR endpoint]/$import
```

## Request

The import operation supports two request formats:

1. **JSON manifest format** - Aligned with
   the [SMART Bulk Data Import](https://github.com/smart-on-fhir/bulk-import)
   specification
2. **FHIR Parameters format** - Standard FHIR Parameters resource

### JSON manifest format

Send a request with `Content-Type: application/json` containing a JSON manifest:

```json
{
    "inputFormat": "application/fhir+ndjson",
    "inputSource": "https://example.com/source-system",
    "input": [
        {
            "type": "Patient",
            "url": "s3a://my-bucket/fhir/Patient.ndjson"
        },
        {
            "type": "Observation",
            "url": "s3a://my-bucket/fhir/Observation.ndjson"
        }
    ],
    "mode": "overwrite"
}
```

#### Manifest fields

| Field         | Cardinality | Description                                                                     |
|---------------|-------------|---------------------------------------------------------------------------------|
| `inputFormat` | 1..1        | The MIME type of the source files. See [Supported formats](#supported-formats). |
| `inputSource` | 1..1        | A URI identifying the source system of the data.                                |
| `input`       | 1..*        | An array of input file specifications.                                          |
| `input.type`  | 1..1        | The FHIR resource type contained in the file.                                   |
| `input.url`   | 1..1        | The URL where the source file can be retrieved.                                 |
| `mode`        | 0..1        | Either `overwrite` (default) or `merge`. See [Import modes](#import-modes).     |

### FHIR Parameters format

Send a request with `Content-Type: application/fhir+json` containing a FHIR
Parameters resource:

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "inputSource",
            "valueString": "https://example.com/source-system"
        },
        {
            "name": "inputFormat",
            "valueCoding": {
                "code": "application/fhir+ndjson"
            }
        },
        {
            "name": "saveMode",
            "valueCoding": {
                "code": "overwrite"
            }
        },
        {
            "name": "input",
            "part": [
                {
                    "name": "resourceType",
                    "valueCoding": {
                        "code": "Patient"
                    }
                },
                {
                    "name": "url",
                    "valueUrl": "s3a://my-bucket/fhir/Patient.ndjson"
                }
            ]
        },
        {
            "name": "input",
            "part": [
                {
                    "name": "resourceType",
                    "valueCoding": {
                        "code": "Observation"
                    }
                },
                {
                    "name": "url",
                    "valueUrl": "s3a://my-bucket/fhir/Observation.ndjson"
                }
            ]
        }
    ]
}
```

#### Parameters

| Name                 | Cardinality | Type   | Description                                                            |
|----------------------|-------------|--------|------------------------------------------------------------------------|
| `inputSource`        | 1..1        | string | A URI identifying the source system of the data.                       |
| `inputFormat`        | 0..1        | Coding | The format of the source files. Defaults to `application/fhir+ndjson`. |
| `saveMode`           | 0..1        | Coding | Either `overwrite` (default) or `merge`.                               |
| `input`              | 1..*        | -      | Contains parts describing each input file.                             |
| `input.resourceType` | 1..1        | Coding | The FHIR resource type contained in the file.                          |
| `input.url`          | 1..1        | url    | The URL where the source file can be retrieved.                        |

## Import modes

| Mode        | Description                                                                                                                                                    |
|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `overwrite` | All existing resources of the specified type are deleted and replaced with the contents of the source file. This is the default.                               |
| `merge`     | Existing resources are matched with resources in the source file based on their ID. Existing resources are updated and new resources are added as appropriate. |

## Response

On successful completion, the operation returns a FHIR Parameters resource:

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "transactionTime",
            "valueInstant": "2025-01-15T10:30:00.000Z"
        },
        {
            "name": "request",
            "valueUrl": "https://pathling.example.com/fhir/$import"
        },
        {
            "name": "output",
            "part": [
                {
                    "name": "inputUrl",
                    "valueUrl": "s3a://my-bucket/fhir/Patient.ndjson"
                }
            ]
        },
        {
            "name": "output",
            "part": [
                {
                    "name": "inputUrl",
                    "valueUrl": "s3a://my-bucket/fhir/Observation.ndjson"
                }
            ]
        }
    ]
}
```

## Python example

The following Python script demonstrates how to invoke the import operation with
asynchronous polling.

Run the script using [uv](https://docs.astral.sh/uv/):

```bash
uv run import_client.py
```

### Import client

```python
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///
"""Demonstrates invoking the $import operation with async polling."""

import time
import requests

BASE_URL = "https://pathling.example.com/fhir"
INPUT_SOURCE = "https://example.com/ehr-system"
SOURCES = [
    ("Patient", "s3a://my-bucket/fhir/Patient.ndjson"),
    ("Observation", "s3a://my-bucket/fhir/Observation.ndjson"),
    ("Condition", "s3a://my-bucket/fhir/Condition.ndjson"),
]


def build_parameters():
    """Build a FHIR Parameters request body."""
    parameters = [
        {"name": "inputSource", "valueString": INPUT_SOURCE},
        {"name": "inputFormat", "valueCoding": {"code": "application/fhir+ndjson"}},
        {"name": "saveMode", "valueCoding": {"code": "overwrite"}}
    ]
    for resource_type, url in SOURCES:
        parameters.append({
            "name": "input",
            "part": [
                {"name": "resourceType", "valueCoding": {"code": resource_type}},
                {"name": "url", "valueUrl": url}
            ]
        })
    return {"resourceType": "Parameters", "parameter": parameters}


def poll_job(status_url, timeout=3600):
    """Poll the job status endpoint until completion."""
    start = time.time()
    interval = 2.0

    while time.time() - start < timeout:
        response = requests.get(status_url, headers={"Accept": "application/fhir+json"})

        if response.status_code == 200:
            print("Import completed successfully")
            return response.json()
        elif response.status_code == 202:
            progress = response.headers.get("X-Progress", "unknown")
            print(f"In progress: {progress}")
            time.sleep(interval)
            interval = min(interval * 1.5, 30.0)
        else:
            response.raise_for_status()

    raise TimeoutError(f"Import timed out after {timeout} seconds")


def main():
    """Execute the import operation."""
    import_url = f"{BASE_URL}/$import"
    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": "application/fhir+json",
        "Prefer": "respond-async"
    }

    print(f"Initiating import at {import_url}")
    response = requests.post(import_url, json=build_parameters(), headers=headers)

    if response.status_code == 202:
        status_url = response.headers.get("Content-Location")
        print(f"Job accepted, polling {status_url}")
        result = poll_job(status_url)
        print(f"Result: {result}")
    else:
        response.raise_for_status()


if __name__ == "__main__":
    main()
```
