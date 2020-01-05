---
layout: page
title: Select search parameter
nav_order: 2
parent: Roadmap
grand_parent: Documentation
---

# Select search parameter

This change will include the creation of a search parameter called `select` (see
[Advanced Search](https://hl7.org/fhir/R4/search.html#query)). This search
parameter will take a set of FHIRPath expressions as parameters, and will return
a [Bundle](https://hl7.org/fhir/R4/bundle.html) of resources for which the
expressions all evaluate to `true`.

This will allow for the retrieval of individual resources using the FHIRPath
syntax.

There will also be a modification made to the [aggregate](../aggregate.html)
operation, such that it returns a FHIRPath expression along with each grouped
result. This expression will be able to be used with the `select` search
parameter in order to "drill down" to the resources that make up each individual
aggregate result.

<img src="/images/select.png" 
     srcset="/images/select@2x.png 2x, /images/select.png 1x"
     alt="Drill-down expressions" />

Next: [Summarise operation](./summarise.html)
