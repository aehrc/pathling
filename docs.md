# Overview

Pathling is a set of tools that make it easier to use [FHIR®](https://hl7.org/fhir) and clinical terminology within health data analytics. It is built on [Apache Spark](https://spark.apache.org), and it implements the [SQL on FHIR](https://sql-on-fhir.org) view specification and the [Bulk Data Access](https://hl7.org/fhir/uv/bulkdata/STU2/) implementation guide.

## What can it do?[​](#what-can-it-do "Direct link to What can it do?")

### Query and transformation of FHIR data[​](#query-and-transformation-of-fhir-data "Direct link to Query and transformation of FHIR data")

[FHIR R4](https://hl7.org/fhir) is the dominant standard for exchanging health data. It comes in both [JSON](https://hl7.org/fhir/json.html) or [XML](https://hl7.org/fhir/xml.html) formats, and can contain over 140 different types of resources, such as [Patient](https://hl7.org/fhir/patient.html), [Observation](https://hl7.org/fhir/observation.html), [Condition](https://hl7.org/fhir/condition.html), [Procedure](https://hl7.org/fhir/procedure.html), and many more.

Pathling is capable of reading all the different types of FHIR resources into a format suitable for data analysis tasks. This makes the following things possible:

* Creating SQL-friendly views from FHIR data
* Transforming data into other formats, such as CSV or [Parquet](https://parquet.apache.org/)
* Performing terminology queries against coded fields within the FHIR data

See [Data in and out](/docs/libraries/io.md) and [Running queries](/docs/libraries/running-queries.md) for more information.

### Terminology queries[​](#terminology-queries "Direct link to Terminology queries")

Health data often contains codes from systems such as [SNOMED CT](https://www.snomed.org/what-is-snomed-ct), [LOINC](https://loinc.org/) or [ICD](https://www.who.int/standards/classifications/classification-of-diseases). These codes contain a great deal of information about diagnoses, procedures, observations and many other aspects of a patient's clinical record.

It is common to group these codes based upon their properties, relationships to other codes, or membership within a pre-defined set. Pathling can automate the task of calling out to a [FHIR terminology server](https://hl7.org/fhir/terminology-service.html) to ask questions about the codes within your data.

Examples of the types of questions that can be answered include:

* Is this SNOMED CT procedure code a type of endoscopy?
* Does this LOINC test result code have an analyte of bilirubin?
* Is this ICD-10 code within the pre-defined list of codes within my cohort definition?

See [Terminology functions](/docs/libraries/terminology.md) for more information.

### Server[​](#server "Direct link to Server")

Pathling Server is a FHIR R4 analytics server that exposes a range of functionality for use by applications and implements:

* **SQL on FHIR**: [Run](/docs/server/operations/view-run.md) view definitions to preview tabular projections of FHIR data, then [export](/docs/server/operations/view-export.md) to NDJSON, CSV, or Parquet
* **Bulk Data Access**: [Export data](/docs/server/operations/export.md) at system, patient, or group level using the FHIR Bulk Data Access specification
* **Bulk Import**: [Import data](/docs/server/operations/import.md) from NDJSON, Parquet, or Delta Lake sources, or [sync with another FHIR server](/docs/server/deployment/synchronization.md) that supports bulk export
* **[Bulk Submit](/docs/server/operations/bulk-submit.md)**: An experimental implementation of the new Bulk Submit proposal
* **[FHIRPath Search](/docs/server/operations/search.md)**: Query resources using FHIRPath expressions
* **[CRUD Operations](/docs/server/operations/crud.md)**: Create, read, update, and delete resources

The server is distributed as a Docker image. It supports [authentication](/docs/server/authorization.md) and also can be scaled over a cluster on [Kubernetes](/docs/server/deployment/kubernetes.md) or other Apache Spark clustering solutions.

See [Server](/docs/server.md) for installation and configuration details.

## Licensing and attribution[​](#licensing-and-attribution "Direct link to Licensing and attribution")

Pathling is a product of the [Australian e-Health Research Centre, CSIRO](https://aehrc.csiro.au), licensed under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0). This means that you are free to use, modify and redistribute the software as you wish, even for commercial purposes.

If you use this software in your research, please consider citing our paper, [Pathling: analytics on FHIR](https://doi.org/10.1186/s13326-022-00277-1).

**Pathling is experimental software, use it at your own risk!** You can get a full description of the current set of known issues over on our [GitHub page](https://github.com/aehrc/pathling/issues).
