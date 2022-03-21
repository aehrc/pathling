---
layout: page title: Async nav_order: 5 parent: Documentation
---

# Asynchronous processing

Pathling implements
the [Asynchronous Request Pattern](https://hl7.org/fhir/r4/async.html)
within the FHIR specification, to provide a way to execute long-running requests
and check on their progress using a status endpoint.

Add a `Prefer: respond-async` header to your request to indicate that you want
to use asynchronous processing. A `202 Accepted` response will be returned,
along with a `Content-Location` header indicating the URL of the status
endpoint.

The following operations support async:

- [Import](./operations/import.html)
- [Aggregate](./operations/aggregate.html)
- [Extract](./operations/extract.html)

Async jobs references are stored in memory. If the server is restarted before
the completion of the job, the initiation request will need to be resent.

If you're using JavaScript, you can use the [pathling-client]() NPM package
which features support for calling Pathling operations using the async pattern.

Next: [Caching](./caching.html)