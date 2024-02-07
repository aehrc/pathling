---
sidebar_position: 5
description: Pathling implements the update and batch operations from the FHIR REST API, to allow for the creation and update of individual resources within the server.
---

# Update and batch

Pathling implements the [update](https://hl7.org/fhir/R4/http.html#update) and
[batch](https://hl7.org/fhir/R4/http.html#transaction) operations from the FHIR
REST API, to allow for the creation and update of individual resources within
the server.

The `batch` implementation only supports the use of the `update` operation,
other operations are not supported within batches.

There are a number of configuration values that affect the encoding of
resources, see the [Encoding](../configuration#encoding) section of the
configuration documentation for details.

## Examples

Check out example `update` and `batch` requests in the Postman collection:

<a class="postman-link"
href="https://documenter.getpostman.com/view/634774/UVsQs48s#54f391df-c993-445b-aa2e-f476efcfe4f9">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>
