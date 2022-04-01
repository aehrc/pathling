---
layout: page
title: Extract
nav_order: 3
parent: Operations
grand_parent: Documentation
---

# Extract

[FHIR OperationDefinition](https://pathling.csiro.au/fhir/OperationDefinition/extract-5)

This operation allows a user to create arbitrary tabular extracts from FHIR 
data, by specifying columns in terms of set of FHIRPath expressions that are 
used to populate them. A URL is returned that points to a delimited text file 
that contains the result of executing the expressions against each subject 
resource.

The extract operation is useful for preparing data for use within other tools, 
and helps to alleviate some of the burden of dealing with FHIR data in its raw 
form.

<div class="callout info">
   If you request a column with the <code>Coding</code> type, it will be expressed within the resulting extract using the <a href="../fhirpath/data-types.html#coding">Coding literal format</a>. 
</div>

<div class="callout info">
    The <code>extract</code> operation supports the <a href="../async.html">Asynchronous Request Pattern</a>, which allows you to kick off a long-running request and check on its progress using a status endpoint.
</div>

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

- `column [1..*]` - (string) A FHIRPath expression that defines a column within 
  the result. The context is a single resource of the subject resource type. 
  The expression must return a 
  [materializable type](./fhirpath/data-types.html#materializable-types). 
- `filter [0..*]` - (string) A FHIRPath expression that can be evaluated against
  each resource in the data set to determine whether it is included within the
  result. The context is an individual resource of the subject resource type.
  The expression must evaluate to a Boolean value. Multiple filters are combined
  using AND logic.
- `limit [0..1]` - (integer) The maximum number of rows to return.
  
## Response

The response for the `$extract` operation is a
[Parameters](https://hl7.org/fhir/R4/parameters.html) resource containing the
following parameters:

- `url [1]` - A URL at which the result of the operation can be retrieved.

## Examples

Check out example `extract` requests in the Postman collection:

<a class="postman-link"
href="https://documenter.getpostman.com/view/634774/UVsQs48s#1aa5cb8f-6931-417c-be20-d295a05af8ed">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>

Next: [Update and batch](./update.html)
