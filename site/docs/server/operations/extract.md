---
sidebar_position: 4
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

:::info
The `aggregate` operation supports the [Asynchronous Request Pattern](../async),
which allows you to kick off a long-running request and check on its progress
using a status endpoint.
:::

:::tip
If you request a column with the `Coding` type, it will be expressed within the
resulting extract using
the [Coding literal format](/docs/fhirpath/data-types#coding).
:::

```
GET [FHIR endpoint]/[resource type]/$extract?[parameters...]
```

```
POST [FHIR endpoint]/[resource type]/$extract
```

![Extract](/images/extract.svg#light-mode-only "Extract")
![Extract](/images/extract-dark.svg#dark-mode-only "Extract")

## Request

The request for the `$extract` operation is either a GET request, or a POST 
request containing a [Parameters](https://hl7.org/fhir/R4/parameters.html) 
resource. The following parameters are supported:

- `column [1..*]` - (string) A FHIRPath expression that defines a column within
  the result. The context is a single resource of the subject resource type.
  The expression must return a
  [materializable type](/docs/fhirpath/data-types#materializable-types).
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
