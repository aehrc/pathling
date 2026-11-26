---
slug: bulk-export-benchmark
title: Bulk export performance in Pathling server 3.2.0
authors: [johngrimes]
tags: [server, performance]
---

Health Samurai maintain an open
[FHIR server performance benchmark](https://github.com/HealthSamurai/fhir-server-performance-benchmark)
that compares Aidbox, HAPI FHIR, Medplum, the Microsoft FHIR server, WSO2 and
InterSystems IRIS for Health. One of its suites measures how quickly each server
can export its whole dataset using the bulk data `$export` operation. We ran
that suite against a pre-release build of Pathling server 3.2.0, using the same
data and the same resource limits as the other servers. Pathling exported the
dataset in 54 to 72 seconds on its first run, and in 12 seconds once it had
warmed up.

<!-- truncate -->

## The benchmark

The benchmark runs each server in its own container with a fixed CPU and memory
budget, backed by its own database. Aidbox and HAPI FHIR, the two JVM-based
servers, are given 8 CPUs and 24 GB of memory, with the heap set to 80% of that
limit. The dataset is a set of Synthea transaction bundles,
containing 1,000 patients and two seed bundles of organisations, locations and
practitioners.

The export test is a [k6](https://k6.io/) script that sends one system-level
`$export` request, polls the status URL every two seconds, and records the time
from the kick-off request to the completed manifest. The headline number is
throughput in megabytes per second: the size of the NDJSON output divided by
that time. The export runs last, after the create, read, update and delete
(CRUD), import and search suites.

Health Samurai publish the results of each run on their
[reports page](https://healthsamurai.github.io/fhir-server-performance-benchmark).
These are the export results from the run of 26 November 2026:

| Server         | Duration (s) | Output (MB) | Throughput (MB/s) |
| -------------- | -----------: | ----------: | ----------------: |
| Aidbox         |           70 |       3,252 |              46.3 |
| Medplum        |           36 |       1,020 |              28.2 |
| Microsoft FHIR |           77 |       2,116 |              27.6 |
| HAPI FHIR      |          176 |       2,796 |              15.8 |

WSO2 and IRIS for Health do not support `$export` on their FHIR endpoints, so
they are reported as not supported.

The servers write quite different amounts of NDJSON for the same data, from
about 1 GB to more than 3 GB, depending on what metadata they add and how they
serialise each resource. Throughput in megabytes per second therefore favours
servers that write more. Duration is the better measure for comparing servers
that have loaded the same data.

## How we ran it

We deployed Pathling on a Kubernetes cluster at CSIRO with the same limits as
the JVM servers in the benchmark: 8 CPUs, 24 GiB of memory and a 19 GB heap. The
warehouse was on a node-local disk. The node has an Intel Xeon Gold 6240M CPU at
2.6 GHz, and it was shared with other workloads at the time.

We used the same dataset: the two seed bundles and the first 1,000 patient
bundles, in the order that the benchmark serves them. This comes to 1,630,685
resources across 24 resource types.

The benchmark loads data by posting each bundle to the server as a transaction,
from 20 clients at once. Pathling's batch support is designed for small updates
rather than bulk loading. It does not resolve the `urn:uuid` and conditional
references within a transaction bundle, and it does not coordinate concurrent
writes to the same table. We converted the bundles to NDJSON instead, rewriting
each reference the way that a server processing the transaction would, and
loaded the result with `$import`. The import took about 85 seconds.

We then ran the benchmark's own export script against the server, with the same
settings that the benchmark uses for HAPI FHIR. Pathling keeps export output in
its own storage rather than writing it to S3, so the size of the output was
measured from the files listed in the manifest.

We needed to make one change to the script. It polls the status URL with
`Accept: application/fhir+json`, the same header it sends with the kick-off
request. The
[Bulk Data Access](https://hl7.org/fhir/uv/bulkdata/STU3/en/export.html#bulk-data-status-request)
specification says that a client should ask for `application/json` when
requesting status. Pathling uses the header to choose between the JSON manifest
and the same content as a FHIR Parameters resource, and returns the Parameters
resource when `application/fhir+json` is requested. HAPI FHIR, whose output is
measured from the manifest in the same way, returns the JSON manifest in
response to the same header. We added a few lines to the script to read the
list of output files from the Parameters resource. This does not affect the
timing.

## Results

Every run exported all 1,630,685 resources, as 2,887 MB of NDJSON in 106 files.

| Scenario                               | Duration (s) | Throughput (MB/s) | Resources per second |
| -------------------------------------- | -----------: | ----------------: | -------------------: |
| First export after import              |           54 |              53.2 |               30,000 |
| First export after a restart           |           72 |              40.4 |               22,800 |
| Warm, dataset caching on (the default) |           12 |             239.2 |              134,800 |
| Warm, dataset caching off              |           20 |             143.8 |               81,100 |

The warm figures are the median of three consecutive runs, which all recorded
the same duration. Durations are rounded up to the next two-second poll of the
status URL, so the shortest runs are slightly overstated.

The first export is the closest match to the benchmark's conditions, where each
server runs a single export. On that measure Pathling took 54 seconds when the
export followed the import, and 72 seconds on a freshly started server. This is
comparable to Aidbox and faster than the Microsoft and HAPI FHIR servers, though
our hardware is different and not directly comparable.

Subsequent exports were considerably faster. With its default configuration
Pathling caches each resource table in memory after it has been read, and the
warm runs exported the dataset in 12 seconds. With dataset caching turned off,
the warm runs took 20 seconds, which reflects the time to read the tables from
disk and write the output.

## Why export is fast in Pathling

Pathling stores each resource type as a [Delta Lake](https://delta.io/) table,
with resources encoded into a columnar schema in Parquet files. An export is a
Spark job that reads each table and writes it out as NDJSON. Each table is
processed in partitions, and the partitions are written in parallel across all
of the available cores. This is why the larger resource types were each written
as 8 files on an 8 CPU server. There is no per-resource query, and no paging
through a database result set.

The cost of the first export is mostly incurred once per server process. After
the restart, the second export took 14 seconds, compared with 72 seconds for the
first. We have not profiled this in detail, but we expect most of the difference
to be JVM warm-up and the generation of Spark code for reading and writing each
of the 24 resource types. A server that has already been answering queries for
the same types will have paid some of that cost before the export starts.

## Limitations

These results come from our own run of the benchmark, not from Health Samurai's
infrastructure. The main differences are:

- The hardware is different. The benchmark's host is not described in enough
  detail to match, and our node was shared with other workloads.
- The data was loaded with `$import` rather than the benchmark's import suite.
  The resources are the same, but the identifiers and metadata differ from what
  each server assigns when it processes a transaction.
- We did not run the CRUD and search suites before the export. On the other
  servers these leave the dataset unchanged, but they warm up the server and its
  database caches. In Pathling, the updates and deletes in the CRUD suite would
  also add small files and versions to the Delta tables, which we did not
  measure.
- Output was written to local disk and measured from the manifest, as for HAPI
  FHIR, rather than written to an S3 bucket.

## Getting started

Pathling server 3.2.0 will be available as a Docker image:

```bash
docker run -p 8080:8080 ghcr.io/aehrc/pathling:3
```

For Kubernetes, use the
[Helm chart](https://artifacthub.io/packages/helm/pathling/pathling). The
[export documentation](/docs/server/operations/export) describes the `$export`
operation and its parameters.
