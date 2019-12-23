---
layout: page
title: Import
nav_order: 1
parent: Documentation
---

# Import

Formal definition:
[https://server.pathling.app/fhir/OperationDefinition/import-0](https://server.pathling.app/fhir/OperationDefinition/import-0)

Pathling provides a [FHIR&reg; REST](https://www.hl7.org/fhir/http.html)
interface, and the `$import` operation is an
[extended operation](https://www.hl7.org/fhir/operations.html) defined on that
interface.

This operation allows bulk FHIR [NDJSON](http://ndjson.org/) data to be imported
into the server, making it available for query via other operations such as
[aggregate](./aggregate.html).

Currently Pathling supports retrieval of NDJSON files from
[Amazon S3](https://aws.amazon.com/s3/) (`s3://`),
[HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (`hdfs://`) and
filesystem (`file://`) URLs. Authentication is supported for S3, see
[Configuration and deployment](./deployment.html) for details.

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

Next: [Aggregate](./aggregate.html)
