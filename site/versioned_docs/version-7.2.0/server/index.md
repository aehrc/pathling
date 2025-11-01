---
sidebar_position: 1
sidebar_label: Introduction
description: Pathling provides a server based on the HL7 FHIR standard, implementing special functionality designed to ease the delivery of apps and augment tasks related to health data analytics.
---

# Server

Pathling provides a server based on the
[HL7&reg; FHIR&reg; standard](https://hl7.org/fhir/R4/), implementing special
functionality designed to ease the delivery of apps and augment tasks related to
health data analytics.

![Operations](../../../src/images/operations.svg#light-mode-only "Operations")
![Operations](../../../src/images/operations-dark.svg#dark-mode-only "Operations")

The operations available within Pathling are:

1. [import](/docs/7.2.0/server/operations/import) - Import FHIR data in bulk
   into the
   server,
   making it available for query using the other operations.
2. [search](/docs/7.2.0/server/operations/search) - Retrieve a set of individual
   FHIR
   resources
   that match a set of criteria.
3. [aggregate](/docs/7.2.0/server/operations/aggregate) - Aggregate, group and
   filter
   data - a
   "pivot table as an API".
5. [extract](/docs/7.2.0/server/operations/extract) - Prepare a custom tabular
   extract
   of FHIR
   data, and retrieve it in bulk.
6. [update](/docs/7.2.0/server/operations/update) - Create or update an
   individual
   resource
   within the server.
7. [batch](/docs/7.2.0/server/operations/update) - Create or update a collection
   of
   resources
   within the server.

You can find some examples of how to interact with Pathling in our
[Postman](https://www.getpostman.com/) collection:

<a class="postman-link"
href="https://documenter.getpostman.com/view/634774/UVsQs48s">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>
