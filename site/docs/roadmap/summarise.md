---
layout: page
title: Summarise operation
nav_order: 3
parent: Roadmap
grand_parent: Documentation
---

# Summarise operation

This change will introduce a new operation called `summarise`. This operation is
designed for extracting data for use within other tools, such as statistical and
machine learning models.

The operation takes a set of expressions that define columns in a tabular view
of the data. A URL pointing to a delimited text file is returned, which contains
the result of executing the expressions against each subject resource.

<img src="/images/summarise.png" 
     srcset="/images/summarise@2x.png 2x, /images/summarise.png 1x"
     alt="Summarise operation" />

## Request

The request for the `$summarise` operation is a
[Parameters](https://hl7.org/fhir/R4/parameters.html) resource containing the
following parameters:

- `subjectResource [1..1]` - (code) The subject resource that the expressions
  within this query are evaluated against. Code must be a member of
  [http://hl7.org/fhir/ValueSet/resource-types](http://hl7.org/fhir/ValueSet/resource-types).
- `column [1..*]` - An expression which is used to extract a value from each
  resource.
  - `expression [1..1]` - (string) A FHIRPath expression that defines the
    column. The context is a single resource of the type specified in the
    subjectResource parameter. The expression must evaluate to a primitive
    value. If any columns preceding this column end in an aggregate function,
    this column expression must also.
  - `label [0..1]` - (string) A short description for the column, for display
    purposes.

## Response

The response for the `$summarise` operation is a
[Parameters](https://hl7.org/fhir/R4/parameters.html) resource containing the
following parameters:

- `url [1..1]` - (uri) A URL at which the result of the operation can be
  retrieved.

Next: [APIs for Python and R](./language-apis.html)
