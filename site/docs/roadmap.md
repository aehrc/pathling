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
3. [APIs for Python and R](#apis-for-python-and-r)
4. [Authorisation enhancements](#authorisation-enhancements)
5. [Incremental update](#incremental-update)
6. [Subscriptions](#subscriptions)
7. [Extension content](#extension-content)
8. [Temporal query](#temporal-query)
9. [Multi-tenancy](#multi-tenancy)
10. [Cell suppression](#cell-suppression)

---

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
  
---

## Improved FHIRPath support

Implementation of a number of operators, functions and syntax elements from the
FHIRPath specifications is planned:

- Various aggregate functions (`average`, `covarPop`, `covarSample`, `max`,
  `min`, `mean`, `stddevPop`, `stddevSample`, `sum`, `varPop`, `varSample`)
- Date component functions (`toSeconds`, `toMinutes`, `toHours`, `dayOfMonth`,
  `dayOfWeek`, `weekOfYear`, `toMonthNumber`, `toQuarter`, `toYear`)
- `dateFormat` function
- `select` function (see
  [select](https://hl7.org/fhirpath/#selectprojection-expression-collection))
- `is` and `as` operators (see
  [Types](https://hl7.org/fhirpath/#types))
- `Quantity` data type, including support for unit-aware operations using
  [UCUM](https://unitsofmeasure.org) (see
  [Quantity](https://hl7.org/fhirpath/#types) and
  [Operations](https://hl7.org/fhirpath/#operations))
- `aggregate` function (see
  [aggregate](https://hl7.org/fhirpath/#aggregateaggregator-expression-init-value-value))

---

## APIs for Python and R

Language-specific APIs will be developed that will allow users of Python and R
to access the functionality within Pathling within their own language
environment. This will make it possible to call a Pathling function such as 
[aggregate](./aggregate.html) or [extract](#extract-operation) and get a 
[Pandas](https://pandas.pydata.org/) or R DataFrame representing the result of 
the operation.

It will be important for these libraries to be able to be installed into a local
environment using language-native package management utilities, i.e.
[pip](https://pypi.org/project/pip/) and [CRAN](https://cran.r-project.org/). 
Two variations of this integration could be created:

1. A library that calls an external Pathling server using its REST API
2. A library package that contains a full copy of Pathling, with no mandatory 
   requirement for an external Pathling server

See [(#194) Python integration](https://github.com/aehrc/pathling/issues/194)
and [(#193) R integration](https://github.com/aehrc/pathling/issues/193).

---

## Authorisation enhancements

This change will enhance the existing support for
[SMART](https://hl7.org/fhir/smart-app-launch/index.html) authorisation within
the Pathling server, adding the following capabilities:

- Individual control of read and write operations
- Access control for individual resource types (e.g. `user/DiagnosticReport.read`)
- Control of access to types of operations (e.g. `operation:search`)

See [(#282) Add operation and resource level authorisation](https://github.com/aehrc/pathling/issues/282).

---

## Incremental update

Work is planned to enable the [create](https://hl7.org/fhir/r4/http.html#create), 
[update](https://hl7.org/fhir/r4/http.html#update) and 
[transaction](https://hl7.org/fhir/r4/http.html#transaction) operations within 
the FHIR REST API. These updates would be made available for query in real-time, 
and could be used to consume data from streaming data sources or as a way of 
synchronising with other FHIR servers via [subscriptions](https://www.hl7.org/fhir/R4/subscription.html).

Integration with [Apache Kafka](https://kafka.apache.org/) as a potential
alternate source of incoming data will also be investigated.

See 
[(#162) Individual resource creation and update](https://github.com/aehrc/pathling/issues/162).

---

## Subscriptions

Work is planned to implement
[FHIR Subscriptions](https://www.hl7.org/fhir/R4/subscription.html) within
Pathling. Push messaging relating to changes within the data (using criteria
described using FHIRPath expressions) could be used as an engine for driving
sophisticated alert systems within the clinical setting.

See [(#164) Subscriptions](https://github.com/aehrc/pathling/issues/164).

---

## Extension content

FHIR has baked [extensibility](https://hl7.org/fhir/R4/extensibility.html) into
the specification, and the need to be able to deal with data within `extension`
elements in resources is significant.

This change will update the data import mechanism to capture extension content
within databases, and implement the `extension` FHIRPath function (see
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)), to
facilitate the use of extensions within expressions.

See [(#163) Extension content](https://github.com/aehrc/pathling/issues/163).

---

## Temporal query

Some types of data are captured within the FHIR model using dates and timestamps 
to describe their temporal aspects. Others are updated in place, and 
information about the previous value and the time of update is effectively lost 
when the change is made.

This change will expand upon the work done on [incremental update](#incremental-update) 
to add the ability to query the history of FHIR resources as they were updated 
within the Pathling data store. This will include the ability to query the state 
of a resource at a point in time and compare it to other versions of that 
resource. 

---

## Multi-tenancy

This change will allow for the hosting of multiple databases by a single
Pathling server.

Each database will expose its own FHIR endpoint, for example:

```
myserver.com/database1/fhir
myserver.com/database2/fhir
```

---

## Cell suppression

A feature is planned to enable suppression of grouping values within the
response of the [aggregate operation](./aggregate.html), based upon certain risk
factors such as cell size. This would reduce the risk of re-identification when
using Pathling to deploy aggregate-only data query services.

---

You can see more planned features, in greater detail, on the 
[Pathling project board](https://github.com/aehrc/pathling/projects/1) on 
GitHub.