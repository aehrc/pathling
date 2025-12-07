---
sidebar_position: 5
description: Pathling supports FHIRPath-based search, allowing you to retrieve FHIR resources filtered by one or more FHIRPath expressions.
---

# Search

Pathling extends the search capabilities of FHIR using FHIRPath expressions for
filtering. This allows you to retrieve a set of FHIR resources from the server
filtered by one or more [FHIRPath](/docs/fhirpath) expressions.

Each expression is evaluated against each resource, returning a Boolean value
which determines whether the resource will be included in the search result.

This is implemented as a [named query](https://hl7.org/fhir/R4/search.html#advanced)
called `fhirPath`. Multiple instances of the `filter` parameter are combined
using Boolean AND logic, and multiple expressions can be provided within a
single parameter and delimited by commas to achieve OR logic. In addition to
this, [FHIRPath boolean operators](/docs/fhirpath/operators#boolean-logic) can
be used within expressions.

```
GET [FHIR endpoint]/[resource type]?_query=fhirPath&filter=[FHIRPath expression]...
```

```
POST [FHIR endpoint]/[resource type]/_search
```

## Request

The `filter` parameter is used to specify FHIRPath expressions:

- `filter [1..*]` - (string) A FHIRPath expression that can be evaluated against
  each resource in the data set to determine whether it is included within the
  result. The context is an individual resource of the type that the search is
  being invoked against. The expression must evaluate to a Boolean value.

Parameters are passed in the query portion of the URL for GET requests, and can
be passed either in the URL or in an `application/x-www-form-urlencoded` body
for POST requests.

## Response

Any search request in FHIR returns a
[Bundle](https://hl7.org/fhir/R4/bundle.html) of matching resources.

Pagination links are included for responses which include a large number of
resources. The number of resources returned in a single response is 100 by
default — this can be altered using the `_count` parameter.

See [Search](https://hl7.org/fhir/R4/search.html) in the FHIR specification for
more details.

## Examples

### Simple filter

Retrieve all male patients:

```
GET /fhir/Patient?_query=fhirPath&filter=gender%3D'male'
```

This is the URL-encoded form of `gender='male'`.

### AND logic

Retrieve female patients who are active (multiple `filter` parameters):

```
GET /fhir/Patient?_query=fhirPath&filter=gender%3D'female'&filter=active%3Dtrue
```

This is the URL-encoded form of `gender='female'` and `active=true`.

### OR logic

Retrieve patients who are either male or female (comma-separated within single
parameter):

```
GET /fhir/Patient?_query=fhirPath&filter=gender%3D'male'%2Cgender%3D'female'
```

This is the URL-encoded form of `gender='male',gender='female'`.

### Complex expressions

Retrieve patients born after 1980:

```
GET /fhir/Patient?_query=fhirPath&filter=birthDate%20%3E%20%401980-01-01
```

This is the URL-encoded form of `birthDate > @1980-01-01`.

### Using FHIRPath operators

Retrieve observations with a specific code and value above threshold:

```
GET /fhir/Observation?_query=fhirPath&filter=code.coding.code%3D'8867-4'%20and%20valueQuantity.value%20%3E%20100
```

This is the URL-encoded form of `code.coding.code='8867-4' and valueQuantity.value > 100`.
