---
layout: page
title: SMART authorisation
nav_order: 5
parent: Roadmap
grand_parent: Documentation
---

# SMART Authorisation

This change will introduce support for enforcing
[SMART](https://hl7.org/fhir/smart-app-launch/index.html) authorisation within
the Pathling server.

Additional use cases specific to authorisation for analytic use cases will need
to be considered:

- Controlling access to individual records, versus aggregate data only
- Obfuscation of aggregate results to reduce the risk of re-identification
- How the presence of an associated
  [Consent](https://hl7.org/fhir/R4/consent.html) resource might influence how
  access to data is authorised

Next: [Extension content](./extensions.html)
