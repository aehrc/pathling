---
layout: page
title: Operations
nav_order: 1
parent: Documentation
has_children: true
---

# Operations

Pathling provides a [FHIR&reg; REST](https://hl7.org/fhir/R4/http.html)
interface, including operations to query and update the data within the server.

<img src="/images/analytics-api.png"
srcset="/images/analytics-api@2x.png 2x, /images/analytics-api.png 1x"
width="600"
alt="FHIR Analytics API" />

The operations available within Pathling are:

1. [import](./import.html) - Import FHIR data in bulk into the server, making it
   available for query using the other operations.
2. [search](./search.html) - Retrieve a set of individual FHIR resources that
   match a set of criteria, described using expressions.
3. [aggregate](./aggregate.html) - A "pivot table as an API", able to take in a
   set of expressions that describe aggregations, groupings and filters and
   return grouped aggregate data.
4. [extract](./extract.html) - Describe a custom tabular extract of FHIR data,
   and retrieve it in bulk.
5. [update](./update.html) - Create or update an individual resource within the
   server.
5. [batch](./update.html) - Create or update a collection of resources within
   the server.

You can find some examples of how to interact with Pathling in our
[Postman](https://www.getpostman.com/) collection:

<a class="postman-link"
href="https://documenter.getpostman.com/view/634774/UVsQs48s">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>
