---
sidebar_position: 3
description: The aggregate operation allows a user to perform aggregate queries on data held within the Pathling FHIR server.
---

# Aggregate

[FHIR OperationDefinition](https://pathling.csiro.au/fhir/OperationDefinition/aggregate-7)

This operation allows a user to perform aggregate queries on data held within
the FHIR server. You call the operation by specifying aggregation, grouping and
filter expressions, and grouped results are returned.

The aggregate operation is useful for exploratory data analysis, as well as
powering visualizations and other summarized views of the data. Drill-down
expressions returned within the response can be used with the [search](./search)
operation to retrieve the resources that make up each grouped result.

:::note
The `aggregate` operation supports the [Asynchronous Request Pattern](../async),
which allows you to kick off a long-running request and check on its progress
using a status endpoint.
:::

```
GET [FHIR endpoint]/[resource type]/$aggregate?[parameters...]
```

```
POST [FHIR endpoint]/[resource type]/$aggregate
```

![Aggregate](../../../src/images/aggregate.svg#light-mode-only "Aggregate")
![Aggregate](../../../src/images/aggregate-dark.svg#dark-mode-only "Aggregate")

## Request

The request for the `$aggregate` operation is either a GET request, or a POST 
request containing a [Parameters](https://hl7.org/fhir/R4/parameters.html) 
resource. The following parameters are supported:

- `aggregation [1..*]` - (string) A FHIRPath expression which is used to
  calculate a summary value from each grouping. The context is a collection of
  resources of the subject resource type. The expression must return a
  [materializable type](/docs/fhirpath/data-types#materializable-types) and also
  be
  singular.
- `grouping [0..*]` - (string) A FHIRPath expression that can be evaluated
  against each resource in the data set to determine which groupings it should
  be counted within. The context is an individual resource of the subject
  resource type. The expression must return a
  [materializable type](/docs/fhirpath/data-types#materializable-types).
- `filter [0..*]` - (string) A FHIRPath expression that can be evaluated against
  each resource in the data set to determine whether it is included within the
  result. The context is an individual resource of the subject resource type.
  The expression must evaluate to a Boolean value. Multiple filters are combined
  using AND logic.
  
## Response

The response for the `$aggregate` operation is a
[Parameters](https://hl7.org/fhir/R4/parameters.html) resource containing the
following parameters:

- `grouping [0..*]` - The grouped results of the aggregations requested in the 
  query. There will be one grouping for each distinct combination of values 
  determined by executing the grouping expressions against each of the resources 
  within the filtered set of subject resources.
  - `label [0..*]` - ([Type](https://hl7.org/fhir/R4/datatypes.html#primitive))
    The set of descriptive labels that describe this grouping, corresponding to
    those requested in the query. There will be one label for each grouping
    within the query, and the type of each label will correspond to the type
    returned by the expression of the corresponding grouping. A grouping
    expression that results in an empty collection will yield a null label,
    which is represented within FHIR as the absence of a value.
  - `result [1..*]` - ([Type](https://hl7.org/fhir/R4/datatypes.html#primitive))
    The set of values that resulted from the execution of the aggregations that
    were requested in the query. There will be one result for each aggregation
    within the query, and the type of each result will correspond to the type
    returned by the expression of the corresponding aggregation.
  - `drillDown [0..1]` - (string) A FHIRPath expression that can be used as a
    filter to retrieve the set of resources that are members of this grouping.
    This will be omitted if there were no groupings or filters passed within the
    query.

## Examples

Check out example `aggregate` requests in the Postman collection:

<a class="postman-link"
href="https://documenter.getpostman.com/view/634774/UVsQs48s#83ef69d8-0cb7-43c2-9f43-f55ffb3ed940">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>
