# Create, read, update, and delete

Pathling implements the [create](https://hl7.org/fhir/R4/http.html#create), [read](https://hl7.org/fhir/R4/http.html#read), [update](https://hl7.org/fhir/R4/http.html#update), and [delete](https://hl7.org/fhir/R4/http.html#delete) operations from the FHIR REST API, for managing individual resources within the server.

There are a number of configuration values that affect the encoding of resources, see the [Encoding](/docs/server/configuration.md#encoding) section of the configuration documentation for details.

## Create operation[​](#create-operation "Direct link to Create operation")

The create operation allows you to create a new resource. The server always generates a new UUID for the resource, ignoring any client-provided ID.

```
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

* HTTP status `201 Created`
* A `Location` header with the URL of the newly created resource
* The created resource with the server-generated ID

```
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

## Read operation[​](#read-operation "Direct link to Read operation")

The read operation retrieves a single resource by its ID.

```
GET /fhir/Patient/example-patient-1
```

The response will be:

* HTTP status `200 OK` with the resource
* HTTP status `404 Not Found` if the resource does not exist

```
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

## Update operation[​](#update-operation "Direct link to Update operation")

The update operation allows you to create or update a resource with a specific ID. If the resource exists, it will be updated; if not, it will be created with the specified ID.

```
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

## Delete operation[​](#delete-operation "Direct link to Delete operation")

The delete operation allows you to remove a resource from the server.

```
DELETE /fhir/Patient/example-patient-1
```

The response will be:

* HTTP status `204 No Content` if the resource was successfully deleted
* HTTP status `404 Not Found` if the resource does not exist

Note that delete operations are not idempotent in Pathling - attempting to delete a resource that has already been deleted will return a `404` error.

## Batch operations[​](#batch-operations "Direct link to Batch operations")

To perform multiple create, update, or delete operations in a single request, see the [Batch](/docs/server/operations/batch.md) documentation.
