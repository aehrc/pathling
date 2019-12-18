---
layout: page
title: FHIRPath
nav_order: 0
parent: Documentation
has_children: true
---

# FHIRPath

Pathling leverages the [FHIRPath](https://hl7.org/fhirpath/2018Sep/index.html)
language in order to abstract away some of the complexity of navigating and
interacting with FHIR data structures.

FHIRPath is actually a model-independent language at its core. For this reason,
the FHIRPath language is defined in two places:

- [https://hl7.org/fhirpath/2018Sep/index.html](https://hl7.org/fhirpath/2018Sep/index.html) -
  The core FHIRPath specification
- [https://hl7.org/fhir/R4/fhirpath.html](https://hl7.org/fhir/R4/fhirpath.html) -
  The FHIRPath page within the FHIR R4 specification, which binds FHIRPath to
  FHIR and defines some FHIR-specific functions and behaviour

The syntax and functions supported by Pathling are described in this document,
with links back to the relevant sections of the FHIRPath specifications.

For convenience, this document does contain redundant information copied from
the source specification. Any variations from the specification are highlighted.

Recommended reading from the FHIRPath specification:

- [Overview](https://hl7.org/fhirpath/2018Sep/index.html#overview) and
- [Navigation model](https://hl7.org/fhirpath/2018Sep/index.html#navigation-model).
