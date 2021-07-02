---
layout: page
title: Search
nav_order: 2
parent: Documentation
---

# Search

[FHIR OperationDefinition](https://pathling.csiro.au/fhir/OperationDefinition/search-4)

Pathling provides a [FHIR&reg; REST](https://hl7.org/fhir/R4/http.html)
interface, and defines a
[named search query](https://hl7.org/fhir/R4/search.html#query) on all resource
types called `fhirPath`.

This search query allows the user to retrieve a set of FHIR resources from the
server filtered by one or more [FHIRPath](./fhirpath) expressions. Each
expression is evaluated against each resource, returning a Boolean value which
determines whether the resource will be included in the search result.

As per the [FHIR search](https://hl7.org/fhir/R4/search.html#combining)
specification, multiple instances of the search parameter are combined using
Boolean AND logic, and multiple expressions can be provided within a single
search parameter and delimited by commas to achieve OR logic. In addition to 
this, [FHIRPath boolean operators](./fhirpath/operators.html#boolean-logic) can 
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
be passed either in the URL or in a `application/x-www-form-urlencoded` for POST
requests.

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
   href="https://documenter.getpostman.com/view/634774/S17rx9Af?version=latest#f3490537-b116-4a2e-8428-45fdb6e7d137">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>

Next: [Aggregate](./aggregate.html)
