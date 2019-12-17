---
layout: page
title: Configuration and deployment
nav_order: 4
---

# Configuration and deployment

Pathling is distributed in two forms: as a JAR file, and a Docker image. Both
forms are configured in the same way, through environment variables.

## Environment variables

The following variables can be used to configure a Pathling instance:

- `PATHLING_HTTP_PORT` - (default: `8080`) The port which the server should bind
  to and listen for HTTP connections.
- `PATHLING_WAREHOUSE_URL` - (default: `file:///usr/share/warehouse`) The base
  URL at which Pathling will look for data files, and where it will save data
  received within [import](./import.html) requests. Can be a
  [Amazon S3](https://aws.amazon.com/s3/) (`s3://`),
  [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (`hdfs://`) or
  filesystem (`file://`) URL.
- `PATHLING_SPARK_MASTER_URL` - (default: `local[*]`) Address of the master node
  of an [Apache Spark](https://spark.apache.org/) cluster to use for processing
  data.
- `PATHLING_DATABASE_NAME` - (default: `default`) The subdirectory within the
  warehouse path in which data is read from and stored.
- `PATHLING_EXECUTOR_MEMORY` - (default: `1g`) The quantity of memory available
  for each child task to process data within, in the same format as JVM memory
  strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).
- `PATHLING_TERMINOLOGY_SERVER_URL` - (default:
  `https://r4.ontoserver.csiro.au/fhir`) The endpoint of a
  [FHIR terminology service](https://hl7.org/fhir/terminology-service.html) (R4)
  that the server can use to resolve terminology queries.
- `PATHLING_AWS_ACCESS_KEY_ID` - Authentication details for connecting to a
  protected Amazon S3 bucket.
- `PATHLING_AWS_SECRET_ACCESS_KEY` - Authentication details for connecting to a
  protected Amazon S3 bucket.
- `PATHLING_EXPLAIN_QUERIES` - (default: `false`) Setting this option to `true`
  will enable additional logging relating to the query plan used to execute
  queries.
- `PATHLING_SHUFFLE_PARTITIONS` - (default: `2`) This option controls the number
  of data partitions used to distribute data between child tasks. This can be
  tuned to higher numbers for larger data sets.
- `PATHLING_CORS_ALLOWED_DOMAINS` - (default: `*`) This is a comma-delimited
  list of domain names that controls which domains are permitted to access the
  server per the `Access-Control-Allow-Origin` header within the
  [Cross-Origin Resource Sharing](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS)
  specification.

## Apache Spark integration

Pathling can optionally connect to, or run within, an
[Apache Spark](https://spark.apache.org/) cluster. Pathling runs Spark 2.4.4
(Scala 2.11), with Hadoop version 2.7.7
