# Server

Pathling Server is a FHIR R4 analytics server that exposes a range of functionality for use by applications.

<!-- -->

The server implements:

* **SQL on FHIR**: [Run](/docs/server/operations/view-run.md) view definitions to preview tabular projections of FHIR data, then [export](/docs/server/operations/view-export.md) to NDJSON, CSV, or Parquet
* **Bulk Data Access**: [Export data](/docs/server/operations/export.md) at system, patient, or group level using the FHIR Bulk Data Access specification
* **Bulk Import**: [Import data](/docs/server/operations/import.md) from NDJSON, Parquet, or Delta Lake sources, or [sync with another FHIR server](/docs/server/deployment/synchronization.md) that supports bulk export
* **[Bulk Submit](/docs/server/operations/bulk-submit.md)**: An experimental implementation of the new Bulk Submit proposal
* **[FHIRPath Search](/docs/server/operations/search.md)**: Query resources using FHIRPath expressions
* **[CRUD Operations](/docs/server/operations/crud.md)**: Create, read, update, and delete resources

The server is distributed as a Docker image. It supports [authentication](/docs/server/authorization.md) and can be scaled over a cluster on [Kubernetes](/docs/server/deployment/kubernetes.md) or other Apache Spark clustering solutions.

## Getting started[​](#getting-started "Direct link to Getting started")

* [Getting started](/docs/server/getting-started.md) - Quick start guide with Docker
* [Configuration](/docs/server/configuration.md) - Environment variables and server settings
* [Deployment](/docs/server/deployment/kubernetes.md) - Kubernetes and AWS deployment options
