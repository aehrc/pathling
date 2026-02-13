# FHIR batch and transaction processing

Detailed specifications for processing multiple operations in a single request.

## Contents

- [Overview](#overview)
- [Request format](#request-format)
- [Batch processing](#batch-processing)
- [Transaction processing](#transaction-processing)
- [Conditional references](#conditional-references)
- [Response format](#response-format)
- [Error handling](#error-handling)

## Overview

FHIR supports two modes for processing multiple operations:

| Mode        | Atomicity   | Failure handling                        |
| ----------- | ----------- | --------------------------------------- |
| Batch       | Independent | Each entry succeeds/fails independently |
| Transaction | Atomic      | All succeed or all fail                 |

**Request:** `POST [base]`

**Content-Type:** `application/fhir+json`

**Request body:** Bundle with `type = batch` or `type = transaction`

## Request format

```json
{
    "resourceType": "Bundle",
    "type": "transaction",
    "entry": [
        {
            "fullUrl": "urn:uuid:abc123",
            "resource": {
                "resourceType": "Patient",
                "name": [{ "family": "Smith" }]
            },
            "request": {
                "method": "POST",
                "url": "Patient"
            }
        },
        {
            "request": {
                "method": "GET",
                "url": "Patient/existing-id"
            }
        }
    ]
}
```

### Entry structure

Each entry requires a `request` element:

| Field                     | Required           | Description                                       |
| ------------------------- | ------------------ | ------------------------------------------------- |
| `request.method`          | Yes                | HTTP method (GET, POST, PUT, PATCH, DELETE, HEAD) |
| `request.url`             | Yes                | Relative URL for the operation                    |
| `request.ifNoneMatch`     | No                 | Conditional read (ETag)                           |
| `request.ifModifiedSince` | No                 | Conditional read (date)                           |
| `request.ifMatch`         | No                 | Conditional update (ETag)                         |
| `request.ifNoneExist`     | No                 | Conditional create (search params)                |
| `resource`                | For POST/PUT/PATCH | The resource to create/update                     |
| `fullUrl`                 | Recommended        | Temporary ID or permanent URL                     |

### Temporary IDs

Use `urn:uuid:` or `urn:oid:` URIs as temporary IDs for new resources:

```json
{
  "fullUrl": "urn:uuid:patient-1",
  "resource": {"resourceType": "Patient", ...},
  "request": {"method": "POST", "url": "Patient"}
}
```

Other entries can reference this before the ID is assigned:

```json
{
    "resource": {
        "resourceType": "Observation",
        "subject": { "reference": "urn:uuid:patient-1" }
    },
    "request": { "method": "POST", "url": "Observation" }
}
```

## Batch processing

**Bundle type:** `batch`

**Characteristics:**

- Entries are processed independently
- Order is not guaranteed
- One entry's failure does not affect others
- Always returns `200 OK` (individual statuses in response)

**Use cases:**

- Bulk data loading where partial success is acceptable
- Independent queries combined for efficiency
- Operations on unrelated resources

## Transaction processing

**Bundle type:** `transaction`

**Characteristics:**

- All entries must succeed or all are rolled back
- Processed as a single atomic unit
- Failure returns single OperationOutcome (not Bundle)

### Processing order

Servers must process entries in this order:

1. DELETE operations
2. POST (create) operations
3. PUT/PATCH (update) operations
4. GET/HEAD (read/search) operations
5. Resolve conditional references

### Referential integrity

Within a transaction:

- References between entries are resolved after IDs are assigned
- Circular references are supported
- All references must resolve (within bundle or on server)

**Use cases:**

- Creating related resources that must exist together
- Updates requiring referential integrity
- Atomic business operations

## Conditional references

Transactions support search-based references instead of direct IDs:

```json
{
    "resource": {
        "resourceType": "Observation",
        "subject": {
            "reference": "Patient?identifier=http://example.org|12345"
        }
    },
    "request": { "method": "POST", "url": "Observation" }
}
```

### Resolution rules

| Matches | Result                        |
| ------- | ----------------------------- |
| 0       | Error (transaction fails)     |
| 1       | Replace with actual reference |
| >1      | Error (transaction fails)     |

### Combined with temporary IDs

A conditional reference may match a resource created in the same transaction:

```json
{
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "identifier": [
                    { "system": "http://example.org", "value": "12345" }
                ]
            },
            "request": { "method": "POST", "url": "Patient" }
        },
        {
            "resource": {
                "resourceType": "Observation",
                "subject": {
                    "reference": "Patient?identifier=http://example.org|12345"
                }
            },
            "request": { "method": "POST", "url": "Observation" }
        }
    ]
}
```

## Response format

### Successful batch/transaction

```json
{
  "resourceType": "Bundle",
  "type": "batch-response",
  "entry": [
    {
      "response": {
        "status": "201 Created",
        "location": "Patient/123/_history/1",
        "etag": "W/\"1\"",
        "lastModified": "2024-01-15T10:30:00Z"
      }
    },
    {
      "resource": {"resourceType": "Patient", ...},
      "response": {
        "status": "200 OK"
      }
    }
  ]
}
```

### Response entry structure

| Field                   | Description                                                          |
| ----------------------- | -------------------------------------------------------------------- |
| `response.status`       | HTTP status code and text                                            |
| `response.location`     | Location header for creates                                          |
| `response.etag`         | ETag header                                                          |
| `response.lastModified` | Last-Modified header                                                 |
| `resource`              | Returned resource (for reads, or if `Prefer: return=representation`) |
| `outcome`               | OperationOutcome for errors or warnings                              |

### Entry correspondence

Response entries are in the same order as request entries. Each response entry corresponds to the request entry at the same index.

## Error handling

### Batch errors

Individual failures are reported per-entry:

```json
{
  "resourceType": "Bundle",
  "type": "batch-response",
  "entry": [
    {
      "response": {"status": "201 Created", "location": "Patient/123"}
    },
    {
      "response": {"status": "422 Unprocessable Entity"},
      "outcome": {
        "resourceType": "OperationOutcome",
        "issue": [{"severity": "error", "code": "invalid", ...}]
      }
    }
  ]
}
```

### Transaction errors

Failed transactions return a single OperationOutcome (not a Bundle):

**Response:** `400 Bad Request` or `500 Internal Server Error`

```json
{
    "resourceType": "OperationOutcome",
    "issue": [
        {
            "severity": "error",
            "code": "processing",
            "diagnostics": "Transaction failed: Patient validation error",
            "location": ["Bundle.entry[2].resource"]
        }
    ]
}
```

### Common error codes

| Status | Cause                                  |
| ------ | -------------------------------------- |
| 400    | Invalid Bundle structure               |
| 404    | Referenced resource not found          |
| 409    | Version conflict                       |
| 412    | Conditional reference matched multiple |
| 422    | Validation failure                     |

## Implementation considerations

### Server requirements

- Transactions require ACID-compliant storage
- Batch processing may be parallelised
- Large bundles may require streaming processing
- Set reasonable size limits (report in CapabilityStatement)

### Client best practices

- Use transactions for related resources
- Use batches for independent operations
- Include `fullUrl` for all resources
- Use temporary UUIDs (`urn:uuid:`) for new resources
- Handle partial batch failures appropriately
