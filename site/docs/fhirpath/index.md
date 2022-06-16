---
sidebar_position: 1
sidebar_label: Introduction
---

# FHIRPath

Pathling leverages the [FHIRPath](https://hl7.org/fhirpath/)
language in order to abstract away some of the complexity of navigating and
interacting with FHIR data structures.

FHIRPath is actually a model-independent language at its core. For this reason,
the FHIRPath language is defined in two places:

- The core
  [FHIRPath specification](https://hl7.org/fhirpath/), and;
- The
  [FHIRPath page within the FHIR R4 specification](https://hl7.org/fhir/R4/fhirpath.html),
  which binds FHIRPath to FHIR and defines some FHIR-specific functions and
  behaviour.

The data types, functions and operators supported by Pathling are described in
this document, with links back to the relevant sections of the FHIRPath
specifications.

For convenience, this document does contain some redundant information copied
from the source specification. Where there are variations from the
specification, this document describes the behaviour as implemented within
Pathling.
