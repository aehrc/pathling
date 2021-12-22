---
layout: page
title: Import
nav_order: 0
parent: Operations
grand_parent: Documentation
---

# Import

[FHIR OperationDefinition](https://pathling.csiro.au/fhir/OperationDefinition/import-5)

This operation allows FHIR R4 data to be imported into the server, making it
available for query via other operations such as [search](./search.html) and
[aggregate](./aggregate.html). This operation accepts the
[NDJSON](http://ndjson.org/) format, and links to retrieve that data are
provided rather that sending the data inline within the request itself. This is
to allow for large data sets to be imported efficiently.

Currently Pathling supports retrieval of NDJSON files from
[Amazon S3](https://aws.amazon.com/s3/) (`s3://`),
[HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (`hdfs://`) and
filesystem (`file://`) URLs. Authentication is supported for S3, see
[Configuration](./configuration.html) for details.

<div class="callout info">
    The <code>import</code> operation supports the <a href="https://hl7.org/fhir/r4/async.html">Asynchronous Request Pattern</a>, which allows you to kick off a long-running request and check on its progress using a status endpoint.
</div>

<div class="callout warning">
    The <code>import</code> operation overwrites all existing data, on a per-resource basis. For example, an import operation that includes the <code>Patient</code> resource will delete all existing <code>Patient</code> resources within the Pathling server, replacing them with the contents of the referenced NDJSON file.
</div>

<div class="callout warning">
    Pathling does not currently support the import of recursive element data, such as <code>Questionnaire.item.item</code>. See <a href="https://github.com/aehrc/pathling/issues/322">#322</a> to follow our progress on this.
</div>

```
POST [FHIR endpoint]/$import
```

<img src="/images/import.png" 
     srcset="/images/import@2x.png 2x, /images/import.png 1x"
     alt="Import operation" />

## Request

The request for the `$import` operation is a
[Parameters](https://hl7.org/fhir/R4/parameters.html) resource containing the
following parameters:

- `source [1..*]` - A source FHIR NDJSON file containing resources to be
  included within this import operation. Each file must contain only one type of
  resource.
  - `resourceType [1..1] (code)` - The base FHIR resource type contained within
    this source file. Code must be a member of
    [http://hl7.org/fhir/ValueSet/resource-types](http://hl7.org/fhir/ValueSet/resource-types).
  - `url [1..1] (uri)` - A URL that can be used to retrieve this source file.

## Response

The response from the `$import` operation is an
[OperationOutcome](https://hl7.org/fhir/R4/operationoutcome.html) resource,
which will indicate success or a description of any errors that occurred.

## Examples

Check out example `import` requests in the Postman collection:

<a class="postman-link"
   href="https://documenter.getpostman.com/view/634774/S17rx9Af?version=latest#d5f260da-7eca-4a19-83b2-a944491ba5a6">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>

Next: [Search](./search.html)
