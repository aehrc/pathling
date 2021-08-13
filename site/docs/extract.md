---
layout: page
title: Extract
nav_order: 4
parent: Documentation
---

# Extract

[FHIR OperationDefinition](https://pathling.csiro.au/fhir/OperationDefinition/extract-4)

Pathling provides a [FHIR&reg; REST](https://hl7.org/fhir/R4/http.html)
interface, and the `$extract` operation is an
[extended operation](https://hl7.org/fhir/R4/operations.html) defined on that
interface.

This operation allows a user to create arbitrary tabular extracts from FHIR 
data, by specifying columns in terms of set of FHIRPath expressions that are 
used to populate them. A URL is returned that points to a delimited text file 
that contains the result of executing the expressions against each subject 
resource.

```
GET [FHIR endpoint]/[resource type]/$extract?[parameters...]
```

```
POST [FHIR endpoint]/[resource type]/$extract
```

<img src="/images/extract.png" 
     srcset="/images/extract@2x.png 2x, /images/extract.png 1x"
     alt="Extract operation" />

## Request

The request for the `$extract` operation is either a GET request, or a POST 
request containing a [Parameters](https://hl7.org/fhir/R4/parameters.html) 
resource. The following parameters are supported:

- `column [1..*]` - (string) A FHIRPath expression that defines the column. The 
  context is a single resource of the type specified in the subjectResource 
  parameter. The expression must return a 
  [materializable type](./fhirpath/data-types.html#materializable-types). 
- `filter [0..*]` - (string) A FHIRPath expression that can be evaluated against 
  each resource in the data set to determine whether it is included within the 
  result. The context is an individual resource of the subject resource type. 
  The expression must evaluate to a Boolean value. Multiple filters are combined 
  using AND logic.
  
## Response

The response for the `$extract` operation is a
[Parameters](https://hl7.org/fhir/R4/parameters.html) resource containing the
following parameters:

- `url [1]` - A URL at which the result of the operation can be retrieved.

## Asynchronous execution

The `$extract` operation supports the 
[asynchronous request pattern](https://hl7.org/fhir/r4/async.html), which allows 
for long running operations to be run outside of the context of a normal HTTP 
request-response interaction.

You can opt-in to asynchronous execution by including the 
`Prefer: respond-async` header in your request. The operation will immediately 
respond with a HTTP status code of `202 Accepted`, along with a 
`Content-Location` header containing the URL relating to the asynchronous job.

You can issue a GET request to the job endpoint to check whether it is finished. 
`202 Accepted` indicates that the job is still in progress, while a `200 OK` 
means the job is finished, and will be accompanied by the response Parameters 
resource.

## Examples

Check out example `extract` requests in the Postman collection:

<a class="postman-link"
   href="https://documenter.getpostman.com/view/634774/S17rx9Af?version=latest#d4afec33-89d8-411c-8e4d-9169b9af42e0">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>

Next: [FHIRPath](./fhirpath)
