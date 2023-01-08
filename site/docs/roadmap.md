# Roadmap

We are continually adding new features to the various different components of
Pathling. If you are interested in specific functionality that doesn't exist
yet, please [get into contact](https://pathling.csiro.au/#contact) with us
or [create an issue](https://github.com/aehrc/pathling/issues/new) to help us
understand your use case.

## Library API: `aggregate` and `extract` operations

This change will make it possible to use
the [aggregate](https://pathling.csiro.au/docs/server/operations/aggregate)
and [extract](https://pathling.csiro.au/docs/server/operations/extract)
operations to aggregate and transform FHIR data from Python, Java and Scala
applications. This feature is currently limited to the server implementation.

See also:

- [Add aggregate function to library API](https://github.com/aehrc/pathling/issues/1060)
- [Add extract operation to library API](https://github.com/aehrc/pathling/issues/1061)

## Ordering

The implementation of an `order` function will allow for the arbitrary
re-ordering of resources and elements within expressions.

See [FHIRPath function: order](https://github.com/aehrc/pathling/issues/448).

## Improved FHIRPath support

Implementation of a number of functions is planned:

### Aggregate functions

- `approxCountDistinct`
- `correlation`
- `countDistinct`
- `covariance[Pop]`
- `kurtosis`
- `last`
- `max`
- `mean`
- `min`
- `percentileApprox`
- `product`
- `skewness`
- `stdDev[Pop]`
- `sumDistinct`
- `variance[Pop]`

### Regular functions

- `contains`
- `startsWith`
- `endsWith`

See [Arbitrary function construction](https://github.com/aehrc/pathling/issues/510).

## R integration

Language-specific APIs will be developed that will allow users of R
to access the functionality within Pathling within their own language
environment.

See [R integration](https://github.com/aehrc/pathling/issues/193).

## Subscriptions

Work is planned to implement
[FHIR Subscriptions](https://www.hl7.org/fhir/R4/subscription.html) within
Pathling. Push messaging relating to changes within the data (using criteria
described using FHIRPath expressions) could be used as an engine for driving
sophisticated alert systems within the clinical setting.

See [Subscriptions](https://github.com/aehrc/pathling/issues/164).

## Project board

You can see more planned features, in greater detail, on the
[Pathling project board](https://github.com/orgs/aehrc/projects/11) on
GitHub.
