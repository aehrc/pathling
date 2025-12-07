---
sidebar_position: 6
description: Pathling implements the update and batch operations from the FHIR REST API, to allow for the creation and update of individual resources within the server.
---

# Update and batch

Pathling implements the [update](https://hl7.org/fhir/R4/http.html#update) and
[batch](https://hl7.org/fhir/R4/http.html#transaction) operations from the FHIR
REST API, to allow for the creation and update of individual resources within
the server.

The `batch` implementation only supports the use of the `update` operation,
other operations are not supported within batches.

Each entry in the batch Bundle must include:

- A `request.method` of `PUT`
- A `request.url` in the format `[resource type]/[id]` (e.g., `Patient/patient-1`)
- A `resource` matching the type and ID specified in the URL

## Examples

### Update single resource

Update or create a Patient resource with a specific ID:

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

### Batch update

Update multiple resources in a single request using a batch Bundle:

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
