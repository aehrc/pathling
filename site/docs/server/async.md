---
sidebar_position: 7
sidebar_label: Async
description: Pathling implements the Asynchronous Request Pattern within the FHIR specification, to provide a way to execute long-running requests and check on their progress using a status endpoint.
---

# Asynchronous processing

Pathling implements
the [Asynchronous Request Pattern](https://hl7.org/fhir/R4/async.html)
within the FHIR specification, to provide a way to execute long-running requests
and check on their progress using a status endpoint.

Add a `Prefer: respond-async` header to your request to indicate that you want
to use asynchronous processing. A `202 Accepted` response will be returned,
along with a `Content-Location` header indicating the URL of the status
endpoint.

The following operations support async:

- [Import](./operations/import)
- [Aggregate](./operations/aggregate)
- [Extract](./operations/extract)

Async job references are stored in memory. If the server is restarted before
the completion of the job, the initiation request will need to be resent.

If you're using JavaScript, you can use
the [pathling-client](https://www.npmjs.com/package/pathling-client) NPM package
which features support for calling Pathling operations using the async pattern.

## Examples

Check out example async requests in the Postman collection:

<a class="postman-link"
href="https://documenter.getpostman.com/view/634774/UVsQs48s#dd55e70a-c67c-4132-9a80-ab5f69e8c2ff">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>
