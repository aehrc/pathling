# SQL on FHIR Operations Reference

SQL on FHIR defines two operations for executing ViewDefinitions and a CapabilityStatement for server discovery.

## CapabilityStatement

Servers expose capabilities at `/metadata`. The CapabilityStatement must declare:

```json
{
    "resourceType": "CapabilityStatement",
    "rest": [
        {
            "mode": "server",
            "resource": [
                {
                    "type": "ViewDefinition",
                    "operation": [
                        {
                            "name": "export",
                            "definition": "http://sql-on-fhir.org/OperationDefinition/$export"
                        },
                        {
                            "name": "run",
                            "definition": "http://sql-on-fhir.org/OperationDefinition/$run"
                        }
                    ]
                }
            ]
        }
    ]
}
```

Optional ViewDefinition CRUD interactions: `read`, `search-type`, `create`, `write`, `patch`, `delete`.

---

## $viewdefinition-run Operation

Synchronous, real-time evaluation of ViewDefinitions.

### Endpoints

```
GET  [base]/ViewDefinition/[id]/$run
POST [base]/ViewDefinition/$run
POST [base]/ViewDefinition/[id]/$run
```

### Input Parameters

| Parameter       | Type           | Required    | Description                                       |
| --------------- | -------------- | ----------- | ------------------------------------------------- |
| `viewReference` | Reference      | Conditional | Reference to server-stored ViewDefinition         |
| `viewResource`  | ViewDefinition | Conditional | Inline ViewDefinition (POST only)                 |
| `_format`       | code           | Yes         | Output format: `json`, `ndjson`, `csv`, `parquet` |
| `header`        | boolean        | No          | Include CSV headers (default: true)               |
| `patient`       | Reference      | No          | Filter by patient                                 |
| `group`         | Reference      | No          | Filter by group membership                        |
| `_since`        | instant        | No          | Resources modified after this time                |
| `_limit`        | integer        | No          | Maximum rows to return                            |
| `resource`      | Resource       | No          | FHIR resources to transform (POST only)           |
| `source`        | string         | No          | External data source URI                          |

At type level, either `viewReference` or `viewResource` is required. At instance level, neither is needed (ViewDefinition inferred from URL).

### Output

Returns `Binary` resource with transformed data in requested format.

### Example: GET with CSV

```http
GET /ViewDefinition/patient-demographics/$run?_format=csv HTTP/1.1
Accept: text/csv
```

Response:

```
HTTP/1.1 200 OK
Content-Type: text/csv

id,birthDate,family,given
pt-1,1990-01-15,Smith,John
pt-2,1985-03-22,Johnson,Mary
```

### Example: POST with Inline ViewDefinition

```http
POST /ViewDefinition/$run?_format=json HTTP/1.1
Content-Type: application/fhir+json
Accept: application/json

{
  "resourceType": "Parameters",
  "parameter": [{
    "name": "viewResource",
    "resource": {
      "resourceType": "ViewDefinition",
      "resource": "Patient",
      "status": "active",
      "select": [{
        "column": [
          {"name": "id", "path": "id"},
          {"name": "birth_date", "path": "birthDate"}
        ]
      }]
    }
  }]
}
```

Response:

```json
[
    { "id": "pt-1", "birth_date": "1990-01-15" },
    { "id": "pt-2", "birth_date": "1985-03-22" }
]
```

### HTTP Status Codes

| Code | Meaning                  |
| ---- | ------------------------ |
| 200  | Success                  |
| 400  | Invalid parameters       |
| 404  | ViewDefinition not found |
| 422  | Invalid ViewDefinition   |
| 500  | Server error             |

---

## $export Operation

Asynchronous bulk export of FHIR data through ViewDefinitions.

### Endpoints

```
POST [base]/ViewDefinition/$export
POST [base]/ViewDefinition/[id]/$export
```

### Input Parameters

| Parameter            | Type            | Required    | Description                                       |
| -------------------- | --------------- | ----------- | ------------------------------------------------- |
| `view`               | BackboneElement | Yes (1..\*) | ViewDefinitions to export                         |
| `view.name`          | string          | No          | Friendly name for output                          |
| `view.viewReference` | Reference       | Conditional | Server-stored ViewDefinition                      |
| `view.viewResource`  | ViewDefinition  | Conditional | Inline ViewDefinition                             |
| `clientTrackingId`   | string          | No          | Client-provided tracking ID                       |
| `_format`            | code            | No          | Output format: `csv`, `ndjson`, `parquet`, `json` |
| `patient`            | Reference       | No          | Filter by patient(s)                              |
| `group`              | Reference       | No          | Filter by group membership                        |
| `_since`             | instant         | No          | Resources updated after this time                 |
| `source`             | string          | No          | External data source URI                          |

### Output Parameters

| Parameter                | Type            | Description                                                   |
| ------------------------ | --------------- | ------------------------------------------------------------- |
| `exportId`               | string          | Server-generated export ID                                    |
| `clientTrackingId`       | string          | Echoed client tracking ID                                     |
| `status`                 | code            | `accepted`, `in-progress`, `completed`, `cancelled`, `failed` |
| `location`               | uri             | Status polling URL                                            |
| `cancelUrl`              | uri             | Cancellation URL                                              |
| `exportStartTime`        | instant         | Start timestamp                                               |
| `exportEndTime`          | instant         | Completion timestamp                                          |
| `exportDuration`         | decimal         | Duration in seconds                                           |
| `estimatedTimeRemaining` | decimal         | Estimated seconds remaining                                   |
| `output`                 | BackboneElement | Per-view results                                              |
| `output.name`            | string          | View name                                                     |
| `output.location`        | uri             | Download URL(s)                                               |

### Async Flow

**1. Kick-off Request:**

```http
POST /ViewDefinition/$export HTTP/1.1
Content-Type: application/fhir+json
Prefer: respond-async

{
  "resourceType": "Parameters",
  "parameter": [
    {
      "name": "view",
      "part": [
        {"name": "name", "valueString": "patient_demographics"},
        {"name": "viewReference", "valueReference": {"reference": "ViewDefinition/patient-demographics"}}
      ]
    },
    {"name": "_format", "valueCode": "parquet"}
  ]
}
```

Response:

```http
HTTP/1.1 202 Accepted
Content-Location: https://server.org/export-status/abc123
```

**2. Poll Status:**

```http
GET /export-status/abc123 HTTP/1.1
```

In-progress response:

```http
HTTP/1.1 202 Accepted
X-Progress: 45%
Retry-After: 30
```

**3. Completion Response:**

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "resourceType": "Parameters",
  "parameter": [
    {"name": "exportId", "valueString": "abc123"},
    {"name": "status", "valueCode": "completed"},
    {
      "name": "output",
      "part": [
        {"name": "name", "valueString": "patient_demographics"},
        {"name": "location", "valueUri": "https://storage.example.org/exports/abc123/patient_demographics.parquet"}
      ]
    }
  ]
}
```

**4. Download Files:**

```http
GET https://storage.example.org/exports/abc123/patient_demographics.parquet
```

### HTTP Status Codes

| Code | Meaning                                |
| ---- | -------------------------------------- |
| 202  | Export accepted or in-progress         |
| 200  | Export completed                       |
| 400  | Invalid parameters                     |
| 404  | ViewDefinition/Patient/Group not found |
| 422  | Invalid ViewDefinition                 |
| 500  | Server error                           |

### Required Headers

- `Prefer: respond-async` (mandatory for kick-off)
- `Content-Type: application/fhir+json`

---

## Output Formats

| Code      | MIME Type                      | Description            |
| --------- | ------------------------------ | ---------------------- |
| `json`    | application/json               | JSON array of objects  |
| `ndjson`  | application/ndjson             | Newline-delimited JSON |
| `csv`     | text/csv                       | Comma-separated values |
| `parquet` | application/vnd.apache.parquet | Apache Parquet         |

Format selection:

- `_format` parameter in request
- `Accept` header (for $run)
