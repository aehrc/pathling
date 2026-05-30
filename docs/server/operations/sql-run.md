# Run SQL query

This operation executes a SQL query against materialised [ViewDefinition](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html) tables within the Pathling server. The query is supplied as a Library resource conforming to the [SQLQuery profile](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-SQLQuery.html) from the SQL on FHIR v2 specification, and is executed against the views it references via `relatedArtifact`.

## Endpoints[​](#endpoints "Direct link to Endpoints")

```
POST [base]/$sqlquery-run
POST [base]/Library/$sqlquery-run
POST [base]/Library/[id]/$sqlquery-run
```

The instance-level form executes a stored Library by ID; the system and type-level forms accept the Library inline or by reference.

## Parameters[​](#parameters "Direct link to Parameters")

| Name             | Cardinality | Type       | Description                                                                                                                                                  |
| ---------------- | ----------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `queryResource`  | 0..1        | Resource   | An inline Library resource conforming to the SQLQuery profile. Mutually exclusive with `queryReference`.                                                     |
| `queryReference` | 0..1        | Reference  | A relative literal (`Library/[id]`) or canonical (`[url]` or `[url]\|[version]`) reference to a stored Library. Resolves against the server's Library store. |
| `_format`        | 0..1        | code       | Output format. Accepts `ndjson` (default), `csv`, `json`, `parquet`, or `fhir`.                                                                              |
| `header`         | 0..1        | boolean    | Include the header row in CSV output. Defaults to `true`.                                                                                                    |
| `_limit`         | 0..1        | integer    | Maximum number of rows to return. Always clamped to the server-configured `maxRows` ceiling.                                                                 |
| `parameters`     | 0..1        | Parameters | Runtime parameter bindings. Each entry's name must match a `Library.parameter` declaration, and its `value[x]` must match the declared FHIR type.            |

Exactly one of `queryResource` and `queryReference` must be supplied to the system and type-level forms. The instance-level form ignores both and uses the Library identified in the URL.

## The Library resource[​](#the-library-resource "Direct link to The Library resource")

The `$sqlquery-run` operation expects a Library that conforms to the SQLQuery profile. The relevant elements are:

* `type` - must include the coding `sql-query` from `https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes`.
* `content` - exactly one entry with `contentType` of `application/sql` and the SQL text Base64-encoded in `data`.
* `relatedArtifact` - one entry per ViewDefinition the query references. The `label` becomes the table name available to the SQL, and `resource` points to the ViewDefinition (relative literal or canonical reference).
* `parameter` - optional declarations of named runtime parameters. Each entry with `use` of `in` must have a `name` and `type`, and the type must be a primitive FHIR type.

Example Library:

```
{
    "resourceType": "Library",
    "status": "active",
    "type": {
        "coding": [
            {
                "system": "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
                "code": "sql-query"
            }
        ]
    },
    "content": [
        {
            "contentType": "application/sql",
            "data": "U0VMRUNUIHBhdGllbnRfaWQsIGdpdmVuX25hbWUsIGZhbWlseV9uYW1lIEZST00gcGF0aWVudHM="
        }
    ],
    "relatedArtifact": [
        {
            "type": "depends-on",
            "label": "patients",
            "resource": "ViewDefinition/patient-demographics"
        }
    ]
}
```

The `data` value above decodes to:

```
SELECT patient_id, given_name, family_name FROM patients
```

## Request format[​](#request-format "Direct link to Request format")

The system-level form accepts a FHIR Parameters resource with the Library nested under `queryResource`:

```
POST [base]/$sqlquery-run HTTP/1.1
Content-Type: application/fhir+json
Accept: application/x-ndjson

{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "queryResource",
            "resource": {
                "resourceType": "Library",
                "status": "active",
                "type": {
                    "coding": [
                        {
                            "system": "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
                            "code": "sql-query"
                        }
                    ]
                },
                "content": [
                    {
                        "contentType": "application/sql",
                        "data": "U0VMRUNUIHBhdGllbnRfaWQgRlJPTSBwYXRpZW50cw=="
                    }
                ],
                "relatedArtifact": [
                    {
                        "type": "depends-on",
                        "label": "patients",
                        "resource": "ViewDefinition/patient-demographics"
                    }
                ]
            }
        }
    ]
}
```

To execute a stored Library by reference instead, use `queryReference`:

```
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "queryReference",
            "valueReference": {
                "reference": "Library/patients-with-conditions"
            }
        }
    ]
}
```

If no Library matches the reference, the server responds with `404`. Supplying neither (or both) of `queryResource` and `queryReference` returns `400`.

## Response formats[​](#response-formats "Direct link to Response formats")

The response uses HTTP chunked transfer encoding so that clients can process results incrementally as they arrive.

### NDJSON[​](#ndjson "Direct link to NDJSON")

When `_format` is `ndjson` (the default), the response is newline-delimited JSON with one row per line:

```
Content-Type: application/x-ndjson

{"patient_id":"pat-1","given_name":"John","family_name":"Smith"}
{"patient_id":"pat-2","given_name":"Jane","family_name":"Johnson"}
```

### CSV[​](#csv "Direct link to CSV")

When `_format` is `csv`, the response is comma-separated values:

```
Content-Type: text/csv

patient_id,given_name,family_name
pat-1,John,Smith
pat-2,Jane,Johnson
```

Set `header=false` to exclude the header row.

### JSON[​](#json "Direct link to JSON")

When `_format` is `json`, the response is a single JSON array of row objects.

### Parquet[​](#parquet "Direct link to Parquet")

When `_format` is `parquet`, the response is a binary Parquet file.

### FHIR Parameters[​](#fhir-parameters "Direct link to FHIR Parameters")

When `_format` is `fhir`, the response is a FHIR Parameters resource (`application/fhir+json`) with one repeating `row` parameter per result row. Each column appears as a part with a typed `value[x]`, mapped from the SQL result schema.

## SQL constraints[​](#sql-constraints "Direct link to SQL constraints")

User SQL is validated before execution. The following are rejected:

* DDL and DML statements (e.g. `CREATE`, `INSERT`, `UPDATE`, `DROP`, `MERGE`).
* References to tables other than those declared in `relatedArtifact`.
* Built-in table-valued functions and arbitrary local file reads.
* Pathling-registered FHIRPath UDFs - these are not part of the SQL surface.

The query may use standard Spark SQL functions, joins, aggregates, and subqueries against the views referenced by the Library.

## Runtime parameters[​](#runtime-parameters "Direct link to Runtime parameters")

A Library may declare named parameters, which are then bound at execution time via the `parameters` input:

```
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "queryReference",
            "valueReference": {
                "reference": "Library/conditions-by-onset"
            }
        },
        {
            "name": "parameters",
            "resource": {
                "resourceType": "Parameters",
                "parameter": [
                    {
                        "name": "minOnsetDate",
                        "valueDate": "2015-01-01"
                    }
                ]
            }
        }
    ]
}
```

Each binding's name must match a declaration on the Library, and its `value[x]` must match the declared FHIR type.

## Resource limits[​](#resource-limits "Direct link to Resource limits")

Two server-configured limits are always applied to a `$sqlquery-run` invocation, regardless of any caller-supplied parameters:

* `pathling.sqlQuery.maxRows` (default `1000000`) - the maximum number of rows that a single response may stream. Clamps `_limit` when that value is larger.
* `pathling.sqlQuery.timeoutSeconds` (default `60`) - the maximum wall-clock time, in seconds, that a query may run before its Spark job group is cancelled.

Long-running queries should use the asynchronous bulk submit path rather than the synchronous `$sqlquery-run` endpoint. See the [configuration reference](/docs/server/configuration.md) for the full list of options.

## Python example[​](#python-example "Direct link to Python example")

The following Python script demonstrates the `$sqlquery-run` operation against a stored Library.

Run the script using [uv](https://docs.astral.sh/uv/):

```
uv run sqlquery_run_client.py
```

### SQL query run client[​](#sql-query-run-client "Direct link to SQL query run client")

```
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///
"""Demonstrates the $sqlquery-run operation."""

import base64
import json
import requests

BASE_URL = "https://pathling.example.com/fhir"


def build_sql_query_library(sql, view_references):
    """Build a Library resource conforming to the SQLQuery profile."""
    encoded = base64.b64encode(sql.encode("utf-8")).decode("ascii")
    return {
        "resourceType": "Library",
        "status": "active",
        "type": {
            "coding": [
                {
                    "system": (
                        "https://sql-on-fhir.org/ig/CodeSystem/"
                        "LibraryTypesCodes"
                    ),
                    "code": "sql-query",
                }
            ]
        },
        "content": [
            {"contentType": "application/sql", "data": encoded}
        ],
        "relatedArtifact": [
            {
                "type": "depends-on",
                "label": label,
                "resource": reference,
            }
            for label, reference in view_references.items()
        ],
    }


def run_sql_query(library, output_format="ndjson", limit=None):
    """Execute a SQLQuery Library and return the streamed response."""
    url = f"{BASE_URL}/$sqlquery-run"
    parameters = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "queryResource", "resource": library},
            {"name": "_format", "valueCode": output_format},
        ],
    }
    if limit is not None:
        parameters["parameter"].append(
            {"name": "_limit", "valueInteger": limit}
        )

    accept = {
        "ndjson": "application/x-ndjson",
        "csv": "text/csv",
        "json": "application/json",
        "fhir": "application/fhir+json",
        "parquet": "application/octet-stream",
    }[output_format]

    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": accept,
    }

    response = requests.post(
        url, json=parameters, headers=headers, stream=True
    )
    response.raise_for_status()
    return response


def main():
    """Execute a join across two materialised views."""
    sql = (
        "SELECT p.given_name, p.family_name, c.condition_name, c.onset_date "
        "FROM patients p "
        "JOIN conditions c "
        "  ON concat('Patient/', p.patient_id) = c.patient_ref "
        "ORDER BY p.family_name, c.onset_date"
    )

    library = build_sql_query_library(
        sql,
        {
            "patients": "ViewDefinition/patient-demographics",
            "conditions": "ViewDefinition/conditions",
        },
    )

    response = run_sql_query(library, output_format="ndjson", limit=20)
    for line in response.iter_lines(decode_unicode=True):
        if line:
            row = json.loads(line)
            print(
                f"{row['given_name']} {row['family_name']}: "
                f"{row['condition_name']} ({row['onset_date']})"
            )


if __name__ == "__main__":
    main()
```
