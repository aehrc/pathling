---
layout: page
title: Aggregate
nav_order: 2
parent: Documentation
---

# Aggregate

Formal definition:
[https://server.pathling.app/fhir/OperationDefinition/aggregate-0](https://server.pathling.app/fhir/OperationDefinition/aggregate-0)

Pathling provides a [FHIR&reg; REST](https://www.hl7.org/fhir/http.html)
interface, and the `$aggregate` operation is an
[extended operation](https://www.hl7.org/fhir/operations.html) defined on that
interface.

This operation allows a user to perform aggregate queries on data held within
the FHIR server by specifying aggregation, grouping and filter expressions.

```
POST [FHIR endpoint]/$aggregate
```

<img src="/images/aggregate.png" 
     srcset="/images/aggregate@2x.png 2x, /images/aggregate.png 1x"/>

## Request

The request for the `$aggregate` operation is a
[Parameters](https://hl7.org/fhir/parameters.html) resource containing the
following parameters:

- `subjectResource [1..1]` - (code) The subject resource that the expressions
  within this query are evaluated against. Code must be a member of
  [http://hl7.org/fhir/ValueSet/resource-types](http://hl7.org/fhir/ValueSet/resource-types).
- `aggregation [1..*]` - An expression which is used to calculate a summary
  value from each grouping.
  - `expression [1..1]` - (string) A FHIRPath expression that defines the
    aggregation. The context is a collection of resources of the type specified
    in the subjectResource parameter. The expression must evaluate to a
    primitive value.
  - `label [0..1]` - (string) A short description for the aggregation, for
    display purposes.
- `grouping [0..*]` - A description of how to group aggregate results.
  - `expression [1..1]` - (string) A FHIRPath expression that can be evaluated
    against each resource in the data set to determine which groupings it should
    be counted within. The context is an individual resource of the type
    specified in the subjectResource parameter. The expression must evaluate to
    a primitive value.
  - `label [0..1]` - (string) A short description for the grouping, for display
    purposes.
- `filter [0..*]` - (string) A FHIRPath expression that can be evaluated against
  each resource in the data set to determine whether it is included within the
  result. The context is an individual resource of the type specified in the
  subjectResource parameter. The expression must evaluate to a Boolean value.
  Multiple filters are combined using AND logic.

## Response

The response for the `$aggregate` operation is a
[Parameters](https://hl7.org/fhir/parameters.html) resource containing the
following parameters:

- `grouping [0..*]` - The grouped results of the aggregations requested in the
  query. There will be one grouping for each distinct combination of values
  determined by executing the grouping expressions against each of the resources
  within the filtered set of subject resources.
  - `label [0..*]` - ([Type](https://hl7.org/fhir/datatypes.html#primitive)) The
    set of descriptive labels that describe this grouping, corresponding to
    those requested in the query. There will be one label for each grouping
    within the query, and the type of each label will correspond to the type
    returned by the expression of the corresponding grouping.
  - `result [1..*]` - ([Type](https://hl7.org/fhir/datatypes.html#primitive))
    The set of values that resulted from the execution of the aggregations that
    were requested in the query. There will be one result for each aggregation
    within the query, and the type of each result will correspond to the type
    returned by the expression of the corresponding aggregation.

Next: [Configuration and deployment](./deployment.html)
