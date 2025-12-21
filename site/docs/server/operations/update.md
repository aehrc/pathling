---
sidebar_position: 6
description: Pathling implements the create, update, delete, and batch operations from the FHIR REST API, to allow for the creation, modification, and deletion of individual resources within the server.
---

# Create, update, delete, and batch

Pathling implements the [create](https://hl7.org/fhir/R4/http.html#create),
[update](https://hl7.org/fhir/R4/http.html#update),
[delete](https://hl7.org/fhir/R4/http.html#delete), and
[batch](https://hl7.org/fhir/R4/http.html#transaction) operations from the FHIR
REST API, to allow for the creation, modification, and deletion of individual
resources within the server.

The `batch` implementation supports `create` (POST), `update` (PUT), and
`delete` (DELETE) operations.

There are a number of configuration values that affect the encoding of
resources, see the [Encoding](../configuration#encoding) section of the
configuration documentation for details.

## Create operation

The create operation allows you to create a new resource. The server always
generates a new UUID for the resource, ignoring any client-provided ID.

```http
POST /fhir/Patient
Content-Type: application/fhir+json

{
    "resourceType": "Patient",
    "name": [
        {
            "family": "Smith",
            "given": ["John"]
        }
    ],
    "gender": "male",
    "birthDate": "1970-01-01"
}
```

The response will include:
- HTTP status `201 Created`
- A `Location` header with the URL of the newly created resource
- The created resource with the server-generated ID

```json
{
    "resourceType": "Patient",
    "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "name": [
        {
            "family": "Smith",
            "given": ["John"]
        }
    ],
    "gender": "male",
    "birthDate": "1970-01-01"
}
```

## Update operation

The update operation allows you to create or update a resource with a specific
ID. If the resource exists, it will be updated; if not, it will be created with
the specified ID.

```http
PUT /fhir/Patient/example-patient-1
Content-Type: application/fhir+json

{
    "resourceType": "Patient",
    "id": "example-patient-1",
    "name": [
        {
            "family": "Smith",
            "given": ["John"]
        }
    ],
    "gender": "male",
    "birthDate": "1970-01-01"
}
```

## Delete operation

The delete operation allows you to remove a resource from the server.

```http
DELETE /fhir/Patient/example-patient-1
```

The response will be:
- HTTP status `204 No Content` if the resource was successfully deleted
- HTTP status `404 Not Found` if the resource does not exist

Note that delete operations are not idempotent in Pathling - attempting to
delete a resource that has already been deleted will return a `404` error.

## Batch operation

The batch operation allows you to perform multiple create, update, and/or delete
operations in a single request.

### Batch requirements

Each entry in the batch Bundle must include:

- A `request.method` of `POST` (create), `PUT` (update), or `DELETE` (delete)
- For create (POST): A `request.url` with just the resource type (e.g.,
  `Patient`)
- For update (PUT) and delete (DELETE): A `request.url` in the format
  `[resource type]/[id]` (e.g., `Patient/patient-1`)
- A `resource` matching the type specified in the URL (not required for DELETE)

### Batch update example

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

### Batch create example

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

### Batch delete example

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

### Mixed batch example

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
