---
layout: page
title: Roadmap
nav_order: 8
parent: Documentation
---

# Roadmap

The roadmap for future development on Pathling is based upon the following
themes:

1. [Improved FHIRPath support](#improved-fhirpath-support)
2. [APIs for Python and R](#apis-for-python-and-r)
3. [Subscriptions](#subscriptions)
4. [CLI mode](#cli-mode)
5. [Temporal query](#temporal-query)

---

## Improved FHIRPath support

Implementation of a number of operators, functions and syntax elements (some 
from the FHIRPath specifications, some novel) is planned:

- [exists](https://hl7.org/fhirpath/#existscriteria-expression-boolean) function
  (see [#326](https://github.com/aehrc/pathling/issues/326))
- `order` function (see [#448](https://github.com/aehrc/pathling/issues/448))
- `property` function (see [#366](https://github.com/aehrc/pathling/issues/366))
- `Quantity` data type, including support for unit-aware operations using
  [UCUM](https://unitsofmeasure.org) (see
  [Quantity](https://hl7.org/fhirpath/#types) and
  [Operations](https://hl7.org/fhirpath/#operations)), and use within
  [Date/time arithmetic](https://hl7.org/fhirpath/#datetime-arithmetic)
- Various aggregate functions (`max`, `min`, `median`)
- Non-reference resource joins (see
  [#338](https://github.com/aehrc/pathling/issues/338))

---

## APIs for Python and R

Language-specific APIs will be developed that will allow users of Python and R
to access the functionality within Pathling within their own language
environment. This will make it possible to call a Pathling function such as
[aggregate](./operations/aggregate.html) or [extract](./operations/extract.html)
and get a [Pandas](https://pandas.pydata.org/) or R DataFrame representing the
result of the operation.

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

## Subscriptions

Work is planned to implement
[FHIR Subscriptions](https://www.hl7.org/fhir/R4/subscription.html) within
Pathling. Push messaging relating to changes within the data (using criteria
described using FHIRPath expressions) could be used as an engine for driving
sophisticated alert systems within the clinical setting.

See [(#164) Subscriptions](https://github.com/aehrc/pathling/issues/164).

---

## CLI mode

This change will create a new execution mode which can be used to invoke 
Pathling operations from the command line. This will remove the need for a 
running server to use Pathling for batch data transformation operations.

See [(#349) CLI mode](https://github.com/aehrc/pathling/issues/349).

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

See [(#350) Temporal query](https://github.com/aehrc/pathling/issues/350).

---

You can see more planned features, in greater detail, on the 
[Pathling project board](https://github.com/aehrc/pathling/projects/1) on 
GitHub.
