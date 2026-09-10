---
sidebar_position: 7
sidebar_label: Batch
description: The batch operation allows you to perform multiple create, update, and delete operations in a single request.
---

# Batch

The [batch](https://hl7.org/fhir/R4/http.html#transaction) operation allows you
to perform multiple create, update, and/or delete operations in a single
request.

For details on individual operations, see the [CRUD](crud) documentation.

## Requirements

Each entry in the batch Bundle must include:

- A `request.method` of `POST` (create), `PUT` (update), or `DELETE` (delete)
- For create (POST): A `request.url` with just the resource type (e.g.,
  `Patient`)
- For update (PUT) and delete (DELETE): A `request.url` in the format
  `[resource type]/[id]` (e.g., `Patient/patient-1`)
- A `resource` matching the type specified in the URL (not required for DELETE)

## Batch update example

Update multiple resources in a single request:

```http
POST /fhir
Content-Type: application/fhir+json

{
    "resourceType": "Bundle",
    "type": "batch",
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-1",
                "name": [{"family": "Smith"}]
            },
            "request": {
                "method": "PUT",
                "url": "Patient/patient-1"
            }
        },
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-2",
                "name": [{"family": "Jones"}]
            },
            "request": {
                "method": "PUT",
                "url": "Patient/patient-2"
            }
        }
    ]
}
```

The response will be a batch-response Bundle containing the outcome for each
entry:

```json
{
    "resourceType": "Bundle",
    "type": "batch-response",
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-1",
                "name": [{"family": "Smith"}]
            },
            "response": {
                "status": "200"
            }
        },
        {
            "resource": {
                "resourceType": "Patient",
                "id": "patient-2",
                "name": [{"family": "Jones"}]
            },
            "response": {
                "status": "200"
            }
        }
    ]
}
```

## Batch create example

Create multiple resources with server-generated IDs:

```http
POST /fhir
Content-Type: application/fhir+json

{
    "resourceType": "Bundle",
    "type": "batch",
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "name": [{"family": "Smith"}]
            },
            "request": {
                "method": "POST",
                "url": "Patient"
            }
        },
        {
            "resource": {
                "resourceType": "Patient",
                "name": [{"family": "Jones"}]
            },
            "request": {
                "method": "POST",
                "url": "Patient"
            }
        }
    ]
}
```

The response will include `201` status codes and server-generated IDs:

```json
{
    "resourceType": "Bundle",
    "type": "batch-response",
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "name": [{"family": "Smith"}]
            },
            "response": {
                "status": "201"
            }
        },
        {
            "resource": {
                "resourceType": "Patient",
                "id": "f1e2d3c4-b5a6-0987-fedc-ba0987654321",
                "name": [{"family": "Jones"}]
            },
            "response": {
                "status": "201"
            }
        }
    ]
}
```

## Batch delete example

Delete multiple resources in a single request:

```http
POST /fhir
Content-Type: application/fhir+json

{
    "resourceType": "Bundle",
    "type": "batch",
    "entry": [
        {
            "request": {
                "method": "DELETE",
                "url": "Patient/patient-1"
            }
        },
        {
            "request": {
                "method": "DELETE",
                "url": "Patient/patient-2"
            }
        }
    ]
}
```

The response will include `204` status codes for successful deletions:

```json
{
    "resourceType": "Bundle",
    "type": "batch-response",
    "entry": [
        {
            "response": {
                "status": "204"
            }
        },
        {
            "response": {
                "status": "204"
            }
        }
    ]
}
```

## Mixed batch example

You can mix create, update, and delete operations in a single batch:

```http
POST /fhir
Content-Type: application/fhir+json

{
    "resourceType": "Bundle",
    "type": "batch",
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "name": [{"family": "NewPatient"}]
            },
            "request": {
                "method": "POST",
                "url": "Patient"
            }
        },
        {
            "resource": {
                "resourceType": "Patient",
                "id": "existing-patient",
                "name": [{"family": "UpdatedName"}]
            },
            "request": {
                "method": "PUT",
                "url": "Patient/existing-patient"
            }
        },
        {
            "request": {
                "method": "DELETE",
                "url": "Patient/patient-to-delete"
            }
        }
    ]
}
```
