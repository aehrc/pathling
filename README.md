<a href="https://pathling.csiro.au">
<picture>
  <source srcset="https://raw.githubusercontent.com/aehrc/pathling/main/media/logo-colour-tight-dark.svg" media="(prefers-color-scheme: dark)"/>
  <img src="https://raw.githubusercontent.com/aehrc/pathling/main/media/logo-colour-detail-tight.svg" alt="Pathling logo" width="300"/>
</picture>
</a>
<br/>
<br/>

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=aehrc_pathling&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=aehrc_pathling)

Pathling is a set of tools that make it easier to
use [FHIR&reg;](https://hl7.org/fhir) and clinical terminology within health
data analytics. It is built on [Apache Spark](https://spark.apache.org), and
it implements the [SQL on FHIR](https://sql-on-fhir.org) view specification and
the [Bulk Data Access](https://hl7.org/fhir/uv/bulkdata/STU2/) implementation
guide.

[**Read the documentation &rarr;**](https://pathling.csiro.au/docs)

## What can it do?

### Query and transformation of FHIR data

[FHIR R4](https://hl7.org/fhir) is the dominant standard for exchanging health
data. It comes in both [JSON](https://hl7.org/fhir/json.html)
or [XML](https://hl7.org/fhir/xml.html) formats, and can contain over 140
different types of resources, such
as [Patient](https://hl7.org/fhir/patient.html),
[Observation](https://hl7.org/fhir/observation.html),
[Condition](https://hl7.org/fhir/condition.html),
[Procedure](https://hl7.org/fhir/procedure.html), and many more.

Pathling is capable of reading all the different types of FHIR resources into a
format suitable for data analysis tasks. This makes the following things
possible:

- Creating SQL-friendly views from FHIR data
- Transforming data into other formats, such as CSV
  or [Parquet](https://parquet.apache.org/)
- Performing terminology queries against coded fields within the FHIR data

See [Data in and out](https://pathling.csiro.au/docs/libraries/io) and
[Running queries](https://pathling.csiro.au/docs/libraries/running-queries) for
more information.

### Terminology queries

Health data often contains codes from systems such
as [SNOMED CT](https://www.snomed.org/snomed-ct/five-step-briefing), [LOINC](https://loinc.org/)
or [ICD](https://www.who.int/standards/classifications/classification-of-diseases).
These codes contain a great deal of information about diagnoses, procedures,
observations and many other aspects of a patient's clinical record.

It is common to group these codes based upon their properties, relationships to
other codes, or membership within a pre-defined set. Pathling can automate the
task of calling out to
a [FHIR terminology server](https://hl7.org/fhir/terminology-service.html) to
ask questions about the codes within your data.

Examples of the types of questions that can be answered include:

- Is this SNOMED CT procedure code a type of endoscopy?
- Does this LOINC test result code have an analyte of bilirubin?
- Is this ICD-10 code within the pre-defined list of codes within my cohort
  definition?

See [Terminology functions](https://pathling.csiro.au/docs/libraries/terminology)
for more information.

### Server

Pathling Server is a FHIR R4 analytics server that exposes a range of
functionality for use by applications and implements:

- **SQL on FHIR**:
  [Run](https://pathling.csiro.au/docs/server/operations/view-run) view
  definitions to
  preview tabular projections of FHIR data,
  then [export](https://pathling.csiro.au/docs/server/operations/view-export) to
  NDJSON, CSV, or Parquet
- **Bulk Data Access
  **: [Export data](https://pathling.csiro.au/docs/server/operations/export) at
  system,
  patient, or group level using the FHIR Bulk Data Access specification
- **Bulk Import
  **: [Import data](https://pathling.csiro.au/docs/server/operations/import)
  from NDJSON,
  Parquet, or Delta Lake sources,
  or [sync with another FHIR server](https://pathling.csiro.au/docs/server/deployment/synchronization)
  that supports bulk export
- **[Bulk Submit](https://pathling.csiro.au/docs/server/operations/bulk-submit)
  **: An experimental
  implementation of the new Bulk Submit proposal
- **[FHIRPath Search](https://pathling.csiro.au/docs/server/operations/search)
  **: Query resources using
  FHIRPath expressions
- **[CRUD Operations](https://pathling.csiro.au/docs/server/operations/crud)**:
  Create, read, update, and
  delete resources

The server is distributed as a Docker image. It
supports [authentication](https://pathling.csiro.au/docs/server/authorization)
and also can be scaled
over a cluster
on [Kubernetes](https://pathling.csiro.au/docs/server/deployment/kubernetes) or
other
Apache Spark clustering solutions.

See [Server](https://pathling.csiro.au/docs/server) for installation and
configuration details.

## Artifact signing

Published Maven artifacts and Helm charts are signed with the following GPG key:

- **Key ID**: `ED48678D`
- **Fingerprint**: `F814 751C 64B5 F5E7 08A8 C73F C3C6 291F ED48 678D`
- **User ID**: `Pathling Developers <pathling@csiro.au>`

The public key is available
on [keys.openpgp.org](https://keys.openpgp.org/search?q=F814751C64B5F5E708A8C73FC3C6291FED48678D).

## Licensing and attribution

Pathling is copyright © 2018-2025, Commonwealth Scientific and Industrial
Research Organisation
(CSIRO) ABN 41 687 119 230. Licensed under
the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

This means that you are free to use, modify and redistribute the software as
you wish, even for commercial purposes.

If you use this software in your research, please consider citing our paper,
[Pathling: analytics on FHIR](https://doi.org/10.1186/s13326-022-00277-1).

**Pathling is experimental software, use it at your own risk!** You can get a
full description of the current set of known issues
[here](https://github.com/aehrc/pathling/issues).
