---
layout: page
title: Configuration and deployment
nav_order: 5
parent: Documentation
---

# Configuration and deployment

Pathling is distributed in two forms: as a JAR file, and a Docker image. Both
forms are configured in the same way, through environment variables.

## Environment variables

### General

- `PATHLING_HTTP_PORT` - (default: `8080`) The port which the server should bind
  to and listen for HTTP connections.
- `PATHLING_VERBOSE_REQUEST_LOGGING` - (default: `false`) Setting this option to
  `true` will enable additional logging of the details of requests to the
  server, and between the server and the terminology service.

### Storage

- `PATHLING_WAREHOUSE_URL` - (default: `file:///usr/share/warehouse`) The base
  URL at which Pathling will look for data files, and where it will save data
  received within [import](./import.html) requests. Can be an
  [Amazon S3](https://aws.amazon.com/s3/) (`s3://`),
  [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (`hdfs://`) or
  filesystem (`file://`) URL.
- `PATHLING_DATABASE_NAME` - (default: `default`) The subdirectory within the
  warehouse path used to read and write data.
- `PATHLING_AWS_ACCESS_KEY_ID` - Authentication details for connecting to a
  protected Amazon S3 bucket.
- `PATHLING_AWS_SECRET_ACCESS_KEY` - Authentication details for connecting to a
  protected Amazon S3 bucket.

### Apache Spark

- `PATHLING_SPARK_MASTER_URL` - (default: `local[*]`) Address of the master node
  of an [Apache Spark](https://spark.apache.org/) cluster to use for processing
  data, see
  [Master URLS](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls).
- `PATHLING_EXECUTOR_MEMORY` - (default: `1g`) The quantity of memory available
  for each child task to process data within, in the same format as JVM memory
  strings with a size unit suffix (`k`, `m`, `g` or `t`) (e.g. `512m`, `2g`).
- `PATHLING_EXPLAIN_QUERIES` - (default: `false`) Setting this option to `true`
  will enable additional logging relating to the query plan used to execute
  queries.
- `PATHLING_SHUFFLE_PARTITIONS` - (default: `2`) This option controls the number
  of data partitions used to distribute data between child tasks. This can be
  tuned to higher numbers for larger data sets. It also controls the granularity
  of requests made to the configured terminology service.

### Terminology service

- `PATHLING_TERMINOLOGY_SERVER_URL` - (default:
  `https://r4.ontoserver.csiro.au/fhir`) The endpoint of a
  [FHIR terminology service](https://hl7.org/fhir/R4/terminology-service.html)
  (R4) that the server can use to resolve terminology queries. Terminology
  functionality can be disabled by setting this variable to an empty string.
- `PATHLING_TERMINOLOGY_SOCKET_TIMEOUT` - (default: `60000`) The maximum period
  (in milliseconds) that the server should wait for incoming data from the
  terminology service.

### Authorisation

- `PATHLING_AUTH_ENABLED` - (default: `false`) Enables
  [SMART](https://hl7.org/fhir/smart-app-launch/index.html) authorisation. If
  this option is set to `true`, the `PATHLING_AUTH_JWKS_URL`,
  `PATHLING_AUTH_ISSUER` and `PATHLING_AUTH_AUDIENCE` options must also be set.
- `PATHLING_AUTH_JWKS_URL` - Provides the URL of a
  [JSON Web Key Set](https://tools.ietf.org/html/rfc7517) from which the signing
  key for incoming bearer tokens can be retrieved, e.g.
  `https://pathling.au.auth0.com/.well-known/jwks.json`.
- `PATHLING_AUTH_ISSUER` - Configures the issuing domain for bearer tokens,
  which will be checked against the claims within incoming bearer tokens, e.g.
  `https://pathling.au.auth0.com/`.
- `PATHLING_AUTH_AUDIENCE` - Configures the audience for bearer tokens, which is
  the FHIR endpoint that tokens are intended to be authorised for, e.g.
  `https://server.pathling.app/fhir`.

### Cross-Origin Resource Sharing (CORS)

- `PATHLING_CORS_ALLOWED_DOMAINS` - (default: `*`) This is a comma-delimited
  list of domain names that controls which domains are permitted to access the
  server per the `Access-Control-Allow-Origin` header within the
  [Cross-Origin Resource Sharing](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS)
  specification.

### Monitoring

- `SENTRY_DSN` - If this variable is set, all errors will be reported to a
  [Sentry](https://sentry.io) service, e.g.
  `https://abc123@sentry.io/123456?servername=server.pathling.app`.

## Server base

There are a number of operations within the Pathling FHIR API that pass back
URLs referring back to API endpoints. The host and protocol components of these
URLs are automatically detected based upon the details of the incoming request.

In some cases it might be desirable to override the hostname and protocol,
particularly where Pathling is being hosted behind some sort of proxy. To
account for this, Pathling also supports the use of the
[X-Forwarded-Host](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Host)
and
[X-Forwarded-Proto](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Proto)
headers to override the hostname and protocol within URLs sent back by the API.

## Authorisation

Pathling can perform the role of a resource server within the
[OAuth 2.0 framework](https://tools.ietf.org/html/rfc6749). The
[SMART App Launch Framework](https://hl7.org/fhir/smart-app-launch/index.html)
is a profile of OAuth 2.0 which is specific to the access of health data.

When authorisation is enabled through onfiguration, Pathling will refuse any
requests which are not accompanied by a valid
[bearer token](https://tools.ietf.org/html/rfc6750). Tokens must meet the
following requirements:

- Conforms to the [JSON Web Token](https://tools.ietf.org/html/rfc7519)
  specification
- Signed using the RSA-256 signing algorithm
- Signed using a public signing key accessible from the configured
  [JSON Web Key Set](https://tools.ietf.org/html/rfc7517) URL
- Contains an
  [audience claim](https://tools.ietf.org/html/rfc7519#section-4.1.3) that
  matches the configured value
- Contains an [issuer claim](https://tools.ietf.org/html/rfc7519#section-4.1.1)
  that matches the configured value
- Contains a value of `user/*.read` within the
  [scope claim](https://hl7.org/fhir/smart-app-launch/scopes-and-launch-context/index.html)

Pathling currently only supports the `user/*.read` scope.
[Future work is planned](./roadmap/smart.html) to expand this to enable control
over the access of individual resource types, aggregate vs individual record
data, and more.

## Apache Spark

Pathling can also be run directly within an Apache Spark cluster as a persistent
application.

For compatibility, Pathling runs Spark 2.4.4 (Scala 2.11), with Hadoop version
2.7.7.

Next: [Roadmap](./roadmap)
