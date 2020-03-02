---
layout: page
title: Extension content
nav_order: 5
parent: Roadmap
grand_parent: Documentation
---

# Extension content

FHIR has baked [extensibility](https://hl7.org/fhir/R4/extensibility.html) into
the specification, and the need to be able to deal with data within `extension`
elements in resources is great.

This change will update the [import](../import.html) to capture extension
content within databases, and implement the `extension` FHIRPath function (see
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)), to
facilitate the use of extensions within expressions.

Next: [Known issues](../known-issues.html)
