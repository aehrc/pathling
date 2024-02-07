---
description: We are continually adding new features to the various different components of Pathling.
---

# Roadmap

We are continually adding new features to the various different components of
Pathling. If you are interested in specific functionality that doesn't exist
yet, please [get into contact](https://pathling.csiro.au/#contact) with us
or [create an issue](https://github.com/aehrc/pathling/issues/new) to help us
understand your use case.

## SQL on FHIR support

We are working on adding support for
executing [SQL on FHIR views](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2)
within both the language libraries and server implementation.

## Additional FHIRPath functions

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

## Ordering

The implementation of an `order` function will allow for the arbitrary
re-ordering of resources and elements within expressions.

See [FHIRPath function: order](https://github.com/aehrc/pathling/issues/448).

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
