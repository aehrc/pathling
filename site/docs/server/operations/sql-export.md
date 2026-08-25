---
sidebar_position: 8
description: The sql-export operation asynchronously exports one or more ViewDefinition, SQLQuery or SQLView subjects to downloadable files.
---

# Export

The `$sql-export` operation is the asynchronous counterpart to
[`$sql-run`](sql-run.md). It exports one or more _subjects_ - in any mixture of
the three kinds - to downloadable files, following the
[FHIR Asynchronous Request Pattern](https://hl7.org/fhir/R4/async.html).

One job carries every subject, which is what makes the outputs comparable:
every subject in a job is computed against a single snapshot of the data, so a
write that lands while the job runs cannot leave two outputs disagreeing with
one another.

## Endpoint

```
POST [base]/$sql-export
Prefer: respond-async
```

The operation is system level only. `GET` is rejected with a `400`, because a
job's subjects and supporting artefacts cannot be expressed in a query string.
The `Prefer: respond-async` header is required; without it the request is
rejected rather than answered synchronously.

## Parameters

| Name                       | Cardinality | Type       | Description                                                                                        |
| -------------------------- | ----------- | ---------- | -------------------------------------------------------------------------------------------------- |
| `subject`                  | 1..\*       | (parts)    | One repetition per artefact to export, in any mixture of kinds. Each produces exactly one output.  |
| `subject.name`             | 0..1        | string     | The output name. Falls back to the artefact's own `name`, then to a generated one. Must be unique. |
| `subject.subjectCanonical` | 0..1        | canonical  | The subject's canonical URL, honouring a `\|version` pin.                                          |
| `subject.subjectReference` | 0..1        | Reference  | A relative reference naming its type: `ViewDefinition/[id]` or `Library/[id]`.                     |
| `subject.subjectResource`  | 0..1        | Resource   | An inline ViewDefinition, SQLQuery or SQLView.                                                     |
| `subject.parameters`       | 0..1        | Parameters | Runtime bindings, for a SQL subject only.                                                          |
| `context`                  | 0..\*       | Resource   | Job-wide inline supporting artefacts, matched by canonical URL. These produce no output.           |
| `clientTrackingId`         | 0..1        | string     | Echoed in the completion manifest.                                                                 |
| `_format`                  | 0..1        | code       | `ndjson` (default), `csv` or `parquet`.                                                            |
| `header`                   | 0..1        | boolean    | Include the header row in CSV output. Defaults to `true`.                                          |
| `patient`                  | 0..\*       | Reference  | Applies to every subject in the job.                                                               |
| `group`                    | 0..\*       | Reference  | Applies to every subject in the job.                                                               |
| `_since`                   | 0..1        | instant    | Applies to every subject in the job.                                                               |
| `source`                   | 0..1        | string     | **Not supported**: an external data source. Supplying it is rejected with a `400`.                 |

Exactly one naming form must be supplied per `subject` repetition. `_limit` is
not offered: an export writes the whole result set.

The `json` and `fhir` formats are not available. `json` is a format this server
has not implemented for export, and is refused as `not-supported`; `fhir` is
meaningless for a bulk file set, and is refused as `invalid`.

## Job guarantees

- **One snapshot.** Every subject reads the Delta table versions pinned when
  the job began, so concurrent writes are invisible to it.
- **One resolution per canonical URL.** Dependency resolution is memoised
  across the job, so an artefact several subjects share is resolved once.
- **One output per subject.** The manifest carries exactly one `output` per
  `subject`, correlated by `name`. There is no ordering guarantee.
- **Validated at kick-off.** Every subject is resolved, named and prepared
  before the job starts, and every problem in the request is reported in one
  `OperationOutcome`. A job that starts is never rejected later at its status
  URL.
- **All or nothing.** A subject that fails fails the whole job, and its partial
  files are removed rather than offered for download.

## Asynchronous flow

Kick-off returns `202 Accepted` with a `Content-Location` status URL. Polling
that URL returns `202` with progress until the job finishes, then `303 See
Other` pointing at the result URL. The result URL returns the completion
manifest, or the failure `OperationOutcome` if the job failed. `DELETE` on the
status URL cancels the job; subsequent polls return `404` and partial files are
cleaned up. Result and download URLs remain valid for at least 24 hours.

## Example

```
POST [base]/$sql-export
Content-Type: application/fhir+json
Prefer: respond-async

{
  "resourceType": "Parameters",
  "parameter": [
    {
      "name": "subject",
      "part": [
        { "name": "name", "valueString": "demographics" },
        {
          "name": "subjectCanonical",
          "valueCanonical": "https://example.org/ViewDefinition/demographics"
        }
      ]
    },
    {
      "name": "subject",
      "part": [
        { "name": "name", "valueString": "smiths" },
        {
          "name": "subjectReference",
          "valueReference": { "reference": "Library/patients-by-family" }
        },
        {
          "name": "parameters",
          "resource": {
            "resourceType": "Parameters",
            "parameter": [{ "name": "family", "valueString": "Smith" }]
          }
        }
      ]
    },
    { "name": "_format", "valueCode": "csv" },
    { "name": "patient", "valueReference": { "reference": "Patient/p1" } }
  ]
}
```

The completion manifest names one output per subject:

```json
{
    "resourceType": "Parameters",
    "parameter": [
        { "name": "exportId", "valueString": "..." },
        { "name": "status", "valueCode": "completed" },
        { "name": "_format", "valueCode": "csv" },
        {
            "name": "output",
            "part": [
                { "name": "name", "valueString": "demographics" },
                {
                    "name": "location",
                    "valueUri": "[base]/$result?job=...&file=demographics.00000.csv"
                }
            ]
        },
        {
            "name": "output",
            "part": [
                { "name": "name", "valueString": "smiths" },
                {
                    "name": "location",
                    "valueUri": "[base]/$result?job=...&file=smiths.00000.csv"
                }
            ]
        }
    ]
}
```

A subject whose result spans several Spark partitions repeats `location` once
per file, all under the one `name`.

## Status codes

| Status                      | Condition                                                                                                     |
| --------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `202 Accepted`              | The job was accepted; poll the `Content-Location` URL.                                                        |
| `400 Bad Request`           | A missing `Prefer` header or a `GET`; no `subject`; a malformed subject; colliding names; `_limit`; `source`. |
| `404 Not Found`             | A subject's canonical or reference resolves to nothing; the status URL of a cancelled job.                    |
| `422 Unprocessable Entity`  | A subject is of no admitted kind, or is conformant but cannot be processed.                                   |
| `500 Internal Server Error` | An unexpected fault, or - on the result URL - the job's own failure outcome.                                  |

A subject whose SQL only Spark's analyser can fault - an unresolved column, an
unknown function, a missing `GROUP BY`, an ambiguous reference - fails the job
rather than the kick-off, since analysis needs the dependency graph
materialised. Its failure outcome is a `422` at the result URL, and its
diagnostics name the failing subject and carry the analyser's own message. As
with `$sql-run`, that message describes the query as rewritten for execution,
so a reported position can differ from the position in the submitted text.

## Conformance

The operation declares the spec canonical
`http://hl7.org/fhir/uv/sql-on-fhir/OperationDefinition/SQLExport` in the server
[CapabilityStatement](https://hl7.org/fhir/R4/capabilitystatement.html), whose
`documentation` states the supported formats and the parameters this server
declines. `cancelUrl` and `estimatedTimeRemaining` are omitted from the
manifest; both are optional upstream.

## Configuration and authorisation

The operation is enabled by `pathling.operations.sqlExportEnabled` (default
`true`) and guarded by the `pathling:sql-export` authority. The same
per-projected-resource and stored-artefact read authorities apply as for
[`$sql-run`](sql-run.md). Jobs are owned by the token subject that started
them; see [authorization](../authorization.md).

## Python client

```python
import time

import requests

base = "https://example.org/fhir"
kick_off = requests.post(
    f"{base}/$sql-export",
    headers={"Content-Type": "application/fhir+json", "Prefer": "respond-async"},
    json={
        "resourceType": "Parameters",
        "parameter": [
            {
                "name": "subject",
                "part": [
                    {"name": "name", "valueString": "demographics"},
                    {
                        "name": "subjectReference",
                        "valueReference": {"reference": "ViewDefinition/demographics"},
                    },
                ],
            },
            {"name": "_format", "valueCode": "parquet"},
        ],
    },
)
kick_off.raise_for_status()
status_url = kick_off.headers["Content-Location"]

while True:
    poll = requests.get(status_url, allow_redirects=False)
    if poll.status_code == 303:
        manifest = requests.get(poll.headers["Location"]).json()
        break
    time.sleep(5)

for parameter in manifest["parameter"]:
    if parameter["name"] == "output":
        for part in parameter["part"]:
            if part["name"] == "location":
                print(part["valueUri"])
```
