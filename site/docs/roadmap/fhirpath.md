---
layout: page
title: Improved FHIRPath support
nav_order: 0
parent: Roadmap
grand_parent: Documentation
---

# Improved FHIRPath support

Implementation of a number of operators, functions and syntax elements from the
FHIRPath specifications is planned:

- `translate` function (see
  [Terminology Service API](https://hl7.org/fhir/R4/fhirpath.html#txapi))
- `select` function (see
  [select](https://hl7.org/fhirpath/2018Sep/index.html#selectprojection-expression-collection))
- Various aggregate functions (`average`, `covarPop`, `covarSample`, `max`,
  `min`, `mean`, `stddevPop`, `stddevSample`, `sum`, `varPop`. `varSample`)
- Date component functions (`toSeconds`, `toMinutes`, `toHours`, `dayOfMonth`,
  `dayOfWeek`, `weekOfYear`, `toMonthNumber`, `toQuarter`, `toYear`)
- `dateFormat` function
- `is` and `as` operators (see
  [Types](https://hl7.org/fhirpath/2018Sep/index.html#types))
- `Quantity` data type, including support for unit-aware operations using
  [UCUM](https://unitsofmeasure.org) (see
  [Quantity](https://hl7.org/fhirpath/2018Sep/index.html#types) and
  [Operations](https://hl7.org/fhirpath/2018Sep/index.html#operations))
- `aggregate` function (see
  [aggregate](https://hl7.org/fhirpath/2018Sep/index.html#aggregateaggregator-expression-init-value-value))

Next: [Multi-tenancy](./multi-tenancy.html)
