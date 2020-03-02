---
layout: page
title: Authorisation enhancements
nav_order: 4
parent: Roadmap
grand_parent: Documentation
---

# Authorisation enhancements

This change will enhance the existing support for
[SMART](https://hl7.org/fhir/smart-app-launch/index.html) authorisation within
the Pathling server.

Functionality will be introduced to account for use cases specific to analytic
use cases, including:

- Controlling access to individual records, versus aggregate data only
- Obfuscation of aggregate results to reduce the risk of re-identification
- How the presence of an associated
  [Consent](https://hl7.org/fhir/R4/consent.html) resource might influence how
  access to data is authorised

Next: [Extension content](./extensions.html)
