---
description: We are continually adding new features to the various different components of Pathling.
wrapperClassName: content-page
---

# Roadmap

We are continually adding new features to the various different components of
Pathling. If you are interested in specific functionality that doesn't exist
yet, please [get into contact](https://pathling.csiro.au/#contact) with us
or [create an issue](https://github.com/aehrc/pathling/issues/new) to help us
understand your use case.

## FHIRPath functionality

We are continually working to expand the set of FHIRPath functions and
operators that Pathling supports. Upcoming additions include:

### Functions

- **Filtering and projection**: `select`, `repeat`
  ([#2388](https://github.com/aehrc/pathling/issues/2388))
- **Combining**: `union`, `combine`
  ([#2384](https://github.com/aehrc/pathling/issues/2384))
- **Subsetting**: `single`, `tail`, `take`, `skip`, `last`, `exclude`,
  `intersect` ([#2378](https://github.com/aehrc/pathling/issues/2378))
- **Existence**: `all`, `subsetOf`, `supersetOf`, `isDistinct`, `distinct`,
  `count`, `allTrue`, `allFalse`, `anyTrue`, `anyFalse`
  ([#2385](https://github.com/aehrc/pathling/issues/2385))
- **String manipulation**: `indexOf`, `substring`, `startsWith`, `endsWith`,
  `contains`, `upper`, `lower`, `replace`, `matches`, `replaceMatches`,
  `length`, `split`, `toChars`, `trim`, `encode`, `decode`, `escape`, `unescape`
  ([#2380](https://github.com/aehrc/pathling/issues/2380))
- **Math**: `abs`, `ceiling`, `floor`, `exp`, `ln`, `log`, `power`, `round`,
  `sqrt`, `truncate` ([#2381](https://github.com/aehrc/pathling/issues/2381))
- **Date and time**: `timeOfDay`, `now`, `today`
  ([#2396](https://github.com/aehrc/pathling/issues/2396))
- **Tree navigation**: `descendants`, `children`
  ([#2382](https://github.com/aehrc/pathling/issues/2382))
- **Reflection**: `type`, `trace`
  ([#2395](https://github.com/aehrc/pathling/issues/2395))
- **Conditional**: `iif`
  ([#2392](https://github.com/aehrc/pathling/issues/2392))
- **Aggregate**: `aggregate`
  ([#2390](https://github.com/aehrc/pathling/issues/2390))
- **FHIR-specific**: `hasValue`
  ([#2430](https://github.com/aehrc/pathling/issues/2430))

### Operators

- **Math**: `*`, `/`, `+`, `-`, `&`, `div`, `mod`
  ([#2399](https://github.com/aehrc/pathling/issues/2399))
- **Equivalence**: `~`, `!~`
  ([#2394](https://github.com/aehrc/pathling/issues/2394))

### Other enhancements

- **Long type**: Support for the Long FHIRPath type and FHIR `integer64`
  ([#2484](https://github.com/aehrc/pathling/issues/2484))
- **$index keyword**: Index access within functions with expression parameters
  ([#2389](https://github.com/aehrc/pathling/issues/2389))
- **Tick quoted identifiers**: Support for `` `identifier` `` syntax
  ([#2153](https://github.com/aehrc/pathling/issues/2153))
- **Factory API**: Support for the FHIR FHIRPath factory API
  ([#2425](https://github.com/aehrc/pathling/issues/2425))
- **Terminology Service API**: The `%terminologies` object for terminology
  operations (`expand`, `lookup`, `validateVS`, `validateCS`, `subsumes`,
  `translate`) ([#2426](https://github.com/aehrc/pathling/issues/2426))

## Parquet on FHIR

We are working on a new interchange specification for the representation of
FHIR within the Parquet format, and plan to implement this within Pathling and
make it the primary persistence format.

The use of this new format will have benefits in terms of interoperability with
other analytic tools as well as performance benefits for Pathling queries over
persisted data.

## Project board

You can see more planned features, in greater detail, on the
[Pathling project board](https://github.com/orgs/aehrc/projects/11) on
GitHub.
