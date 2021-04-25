---
layout: page
title: Roadmap
nav_order: 6
parent: Documentation
---

# Roadmap

The roadmap for future development on Pathling is based upon the following
themes:

1. [Extract operation](#extract-operation)
2. [Improved FHIRPath support](#improved-fhirpath-support)
3. [Cell suppression](#cell-suppression)
4. [Real-time updates](#real-time-updates)
5. [Subscriptions](#subscriptions)
6. [Extension content](#extension-content)
7. [Authorisation enhancements](#authorisation-enhancements)
8. [APIs for Python and R](#apis-for-python-and-r)
9. [Multi-tenancy](#multi-tenancy)

## Extract operation

This change will introduce a new operation called `extract`. This operation is
designed for transforming FHIR data into a flattened form, for use within other 
tools, such as statistical and machine learning models.

The operation takes a set of expressions that define columns in a tabular view
of the data. A URL pointing to a delimited text file is returned, which contains
the result of executing the expressions against each subject resource.

<img src="/images/extract.png" 
     srcset="/images/extract@2x.png 2x, /images/extract.png 1x"
     alt="Extract operation" />

### Request

The request for the `$extract` operation is a
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
- `filter [0..*]` - (string) A FHIRPath expression that can be evaluated against
  each resource in the data set to determine whether it is included within the
  result. The context is an individual resource of the type specified in the
  subjectResource parameter. The expression must evaluate to a Boolean value.
  Multiple filters are combined using AND logic.

### Response

The response for the `$extract` operation is a
[Parameters](https://hl7.org/fhir/R4/parameters.html) resource containing the
following parameters:

- `url [1..1]` - (uri) A URL at which the result of the operation can be
  retrieved.

## Improved FHIRPath support

Implementation of a number of operators, functions and syntax elements from the
FHIRPath specifications is planned:

- `translate` function (see
  [Terminology Service API](https://hl7.org/fhir/R4/fhirpath.html#txapi))
- `select` function (see
  [select](https://hl7.org/fhirpath/#selectprojection-expression-collection))
- Various aggregate functions (`average`, `covarPop`, `covarSample`, `max`,
  `min`, `mean`, `stddevPop`, `stddevSample`, `sum`, `varPop`, `varSample`)
- Date component functions (`toSeconds`, `toMinutes`, `toHours`, `dayOfMonth`,
  `dayOfWeek`, `weekOfYear`, `toMonthNumber`, `toQuarter`, `toYear`)
- `dateFormat` function
- `is` and `as` operators (see
  [Types](https://hl7.org/fhirpath/#types))
- `Quantity` data type, including support for unit-aware operations using
  [UCUM](https://unitsofmeasure.org) (see
  [Quantity](https://hl7.org/fhirpath/#types) and
  [Operations](https://hl7.org/fhirpath/#operations))
- `aggregate` function (see
  [aggregate](https://hl7.org/fhirpath/#aggregateaggregator-expression-init-value-value))

## Real-time updates

Work is planned to address the real-time streaming use case by enabling create
and update operations on the FHIR REST API within Pathling. These operations
would be used by clients to update data on a per-resource basis, or using
batches of related resources. These updates would be made available for query in
real-time.

Integration with [Apache Kafka](https://kafka.apache.org/) as a potential
alternate source of incoming data will also be investigated.

See 
[(#162) Individual resource creation and update](https://github.com/aehrc/pathling/issues/162).

## Subscriptions

Work is planned to implement
[FHIR Subscriptions](https://www.hl7.org/fhir/R4/subscription.html) within
Pathling. Push messaging relating to changes within the data (using criteria
described using FHIRPath expressions) could be used as an engine for driving
sophisticated alert systems within the clinical setting.

See [(#164) Subscriptions](https://github.com/aehrc/pathling/issues/164).

## Extension content

FHIR has baked [extensibility](https://hl7.org/fhir/R4/extensibility.html) into
the specification, and the need to be able to deal with data within `extension`
elements in resources is great.

This change will update the [import](./import.html) to capture extension content
within databases, and implement the `extension` FHIRPath function (see
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)), to
facilitate the use of extensions within expressions.

See [(#163) Extension content](https://github.com/aehrc/pathling/issues/163).

## APIs for Python and R

Language-specific APIs will be developed that will allow users of Python and R
to access the functionality within Pathling within their own language
environment. The ability to run operations using a Spark cluster would remain,
as this is an important capability when dealing with large datasets.

Instead of calling a FHIR API to perform an operation, it will be possible to
call a function and get a Spark DataFrame object returned. The Spark DataFrame
could then be "collected" into a [Pandas](https://pandas.pydata.org/) or R
DataFrame at the point at which it makes sense to bring the data into the local
language environment, in the same way that this is currently done when using
[PySpark](https://spark.apache.org/docs/latest/api/python/index.html) or
[SparkR](https://spark.apache.org/docs/latest/api/R/index.html).

It will be important for these libraries to be able to be installed into a local
environment using language-native package management utilities, i.e.
[pip](https://pypi.org/project/pip/) and [CRAN](https://cran.r-project.org/).

See [(#194) Python integration](https://github.com/aehrc/pathling/issues/194) 
and [(#193) R integration](https://github.com/aehrc/pathling/issues/193).

## Multi-tenancy

This change will allow for the hosting of multiple databases by a single
Pathling server.

Each database will expose its own FHIR endpoint, for example:

```
myserver.com/database1/fhir
myserver.com/database2/fhir
```

## Authorisation enhancements

This change will enhance the existing support for
[SMART](https://hl7.org/fhir/smart-app-launch/index.html) authorisation within
the Pathling server.

Focus will be placed on the following areas:

- Controlling access to individual records, versus aggregate data only
- Obfuscation of aggregate results to reduce the risk of re-identification
- How the presence of an associated
  [Consent](https://hl7.org/fhir/R4/consent.html) resource might influence how
  access to data is authorised
  
See [(#282) Add operation and resource level authorisation](https://github.com/aehrc/pathling/issues/282).

## Cell suppression

A feature is planned to enable suppression of grouping values within the
response of the [aggregate operation](./aggregate.html), based upon certain risk
factors such as cell size. This would reduce the risk of re-identification when
using Pathling to deploy aggregate-only data query services.

You can see more planned features, in greater detail, on the 
[Pathling project board](https://github.com/aehrc/pathling/projects/1) on 
GitHub.