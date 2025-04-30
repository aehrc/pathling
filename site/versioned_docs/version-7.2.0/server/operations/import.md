---
sidebar_position: 1
description: The import operation allows FHIR data to be imported into the server, making it available for query via other operations such as search, aggregate and extract.
---

# Import

[FHIR OperationDefinition](https://pathling.csiro.au/fhir/OperationDefinition/import-7)

This operation allows FHIR R4 data to be imported into the server, making it
available for query via other operations such
as [search](./search), [aggregate](./aggregate) and [extract](./extract). This
operation accepts the [NDJSON](https://hl7.org/fhir/R4/nd-json.html) format, and
links to retrieve that data are provided rather that sending the data inline
within the request itself. This is to allow for large data sets to be imported
efficiently.

Currently Pathling supports retrieval of NDJSON files from
[Amazon S3](https://aws.amazon.com/s3/) (`s3://`),
[HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (`hdfs://`) and
filesystem (`file://`) URLs. Authentication is supported for S3, see
[Configuration](../configuration) for details.

Import sources must be explicitly whitelisted within the configuration, for
security reasons. See the [Import](../configuration#import) section of the
configuration documentation for details.

There are a number of configuration values that affect the encoding of resources
imported by this operation, see the [Encoding](../configuration#encoding)
section of the configuration documentation for details.

:::note
The `import` operation supports the [Asynchronous Request Pattern](../async),
which allows you to kick off a long-running request and check on its progress
using a status endpoint.
:::

```
POST [FHIR endpoint]/$import
```

![Import](../../../../src/images/import.svg#light-mode-only "Import")
![Import](../../../../src/images/import-dark.svg#dark-mode-only "Import")

## Request

The request for the `$import` operation is a
[Parameters](https://hl7.org/fhir/R4/parameters.html) resource containing the
following parameters:

- `source [1..*]` - A source FHIR NDJSON file containing resources to be
  included within this import operation. Each file must contain only one type of
  resource.
    - `resourceType [1..1] (code)` - The base FHIR resource type contained
      within this source file. Code must be a member of
      [http://hl7.org/fhir/ValueSet/resource-types](http://hl7.org/fhir/ValueSet/resource-types).
    - `url [1..1] (uri)` - A URL that can be used to retrieve this source file.
    - `mode [0..1] (code)` - A value of `overwrite` will cause all existing
      resources of the specified type to be deleted and replaced with the
      contents of the source file. A value of `merge` will match existing
      resources with updated resources in the source file based on their ID, and
      either update the existing resources or add new resources as appropriate.
      The default value is `overwrite`.

## Response

The response from the `$import` operation is an
[OperationOutcome](https://hl7.org/fhir/R4/operationoutcome.html) resource,
which will indicate success or a description of any errors that occurred.

## Examples

Check out example `import` requests in the Postman collection:

<a class="postman-link"
href="https://documenter.getpostman.com/view/634774/UVsQs48s#72ee4a60-e701-4d1a-af58-85a762301b6c">
<img src="https://run.pstmn.io/button.svg" alt="Run in Postman"/></a>
