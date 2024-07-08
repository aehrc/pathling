---
sidebar_position: 2
description: Pathling server supports a FHIRPath-based search profile that allows you to retrieve a set of FHIR resources from the server filtered by one or more FHIRPath expressions.
---

# Search

[FHIR OperationDefinition](https://pathling.csiro.au/fhir/OperationDefinition/search-7)

This operation extends the search capabilities of FHIR using a
[search profile](https://hl7.org/fhir/R4/search.html#query) called `fhirPath`,
for use with all resource types.

This search profile allows the user to retrieve a set of FHIR resources from the
server filtered by one or more [FHIRPath](/docs/fhirpath) expressions. Each
expression is evaluated against each resource, returning a Boolean value which
determines whether the resource will be included in the search result.

As per the [FHIR search](https://hl7.org/fhir/R4/search.html#combining)
specification, multiple instances of the search parameter are combined using
Boolean AND logic, and multiple expressions can be provided within a single
search parameter and delimited by commas to achieve OR logic. In addition to
this, [FHIRPath boolean operators](/docs/fhirpath/operators#boolean-logic) can
be used within expressions.

Composite search parameters (using the `$` notation) are not currently
supported.

```
GET [FHIR endpoint]/[resource type]?_query=fhirPath&filter=[FHIRPath expression]...
```

```
POST [FHIR endpoint]/[resource type]/_search
```

## Request

The `fhirPath` named search query defines a single parameter:

- `filter [1..*]` - (string) A FHIRPath expression that can be evaluated against
  each resource in the data set to determine whether it is included within the
  result. The context is an individual resource of the type that the search is
  being invoked against. The expression must evaluate to a Boolean value.

The named query is invoked using a parameter of `_query` with a value of
`fhirPath`.

Parameters are passed in the query portion of the URL for GET requests, and can
be passed either in the URL or in a `application/x-www-form-urlencoded` body for 
POST requests.

## Response

Any search request in FHIR returns a
[Bundle](https://hl7.org/fhir/R4/bundle.html) of matching resources.

Pagination links are included for responses which include a large number of
resources. The number of resources returned in a single response is 100 by
default &#8212; this can be altered using the `_count` parameter.

See [Search](https://hl7.org/fhir/R4/search.html) in the FHIR specification for
more details.

## Examples

Check out example search requests in the Postman collection:

<a class="postman-link"
href="https://documenter.getpostman.com/view/634774/UVsQs48s#9d1a62bd-53e7-42f9-b71f-2dc780c46c9d">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>
