---
description: We are continually adding new features to the various different components of Pathling.
---

# Roadmap

We are continually adding new features to the various different components of
Pathling. If you are interested in specific functionality that doesn't exist
yet, please [get into contact](https://pathling.csiro.au/#contact) with us
or [create an issue](https://github.com/aehrc/pathling/issues/new) to help us
understand your use case.

## Server

We have temporarily removed the server feature from Pathling while we redesign 
it. The new Pathling server will focus on the following features.

### Bulk Data Access Export API

We are working on an implementation of the Export operation within the FHIR
Bulk Data Access implementation guide. This will allow you to use Pathling as a 
way of providing bulk export functionality over your FHIR data. When used with 
our existing bulk client and Kafka features, you will be able to synchronise the 
Pathling server with another FHIR server and use it to provide bulk export 
services.

### SQL on FHIR API

We are working on implementing the draft API within the SQL on FHIR 
implementation guide. This will allow for the execution of view definitions 
through the server API, and the use of views to construct bespoke exports. It 
will also allow for the discovery and management of view definitions through the 
FHIR REST API.

## Parquet on FHIR

We are working on a new interchange specification for the representation of
FHIR within the Parquet format, and plan to implement this within Pathling and
make it the primary persistence format.

The use of this new format will have benefits in terms of interoperability with
other analytic tools as well as performance benefits for Pathling queries over
persisted data.

## Project board

You can see more planned features, in greater detail, on the
[Pathling project board](https://github.com/orgs/aehrc/projects/11) on
GitHub.
