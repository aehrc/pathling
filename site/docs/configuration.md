---
layout: page
title: Configuration
nav_order: 5
parent: Documentation
---

# Configuration

Pathling is distributed in two forms: as a JAR file, and a Docker image. The 
easiest way to configure Pathling is through environment variables.

If for whatever reason environment variables are problematic for your 
deployment, Pathling can be also configured in in a variety of other ways as 
supported by the Spring Boot framework (see 
[Spring Boot Reference Documentation: Externalized Configuration](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config))

## Configuration variables

### General

- `server.port` - (default: `8080`) The port which the server should bind
  to and listen for HTTP connections.
  
- `server.servlet.context-path` - A prefix to add to the API endpoint, e.g. a 
  value of `/foo` would cause the FHIR endpoint to be changed to `/foo/fhir`.
- `pathling.verboseRequestLogging` - (default: `false`) Setting this option to
  `true` will enable additional logging of the details of requests to the
  server, and between the server and the terminology service.
- `pathling.implementationDescription` - (default: 
  `Yet Another Pathling Server`) Controls the content of the 
  `implementation.description` element within the server's 
  [CapabilityStatement](https://hl7.org/fhir/R4/http.html#capabilities).
- `pathling.spark.appName` - (default: `pathling`) Controls the application name 
  that Pathling will be identified as within any Spark cluster that it 
  participates in.
- `pathling.spark.explainQueries` - (default: `false`) If set to true, Spark
  query plans will be written to the logs.
- `JAVA_OPTS` - (default in Docker image: `-Xmx2g`) Allows for the configuration of arbitrary 
  options on the Java VM that Pathling runs within.
  
Additionally, you can set any variable supported by Spring Boot, see 
[Spring Boot Reference Documentation: Common Application properties](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#common-application-properties).

### Import

- `pathling.import.allowableSources` - (default: `file:///usr/share/staging`) A 
  set of URL prefixes which are allowable for use within the import operation.

### Storage

- `pathling.storage.warehouseUrl` - (default: `file:///usr/share/warehouse`) The 
  base URL at which Pathling will look for data files, and where it will save 
  data received within [import](./import.html) requests. Can be an
  [Amazon S3](https://aws.amazon.com/s3/) (`s3://`),
  [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (`hdfs://`) or
  filesystem (`file://`) URL.
- `pathling.storage.databaseName` - (default: `default`) The subdirectory within 
  the warehouse path used to read and write data.
- `pathling.storage.aws.anonymousAccess` - (default: `true`) Public S3 buckets 
  can be accessed by default, set this to false to access protected buckets.
- `pathling.storage.aws.accessKeyId` - Authentication details for connecting to 
  a protected Amazon S3 bucket.
- `pathling.storage.aws.secretAccessKey` - Authentication details for connecting 
  to a protected Amazon S3 bucket.

### Apache Spark

Any Spark configuration variable can be set within Pathling directly. See 
[Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) 
for the full list.

Here are a few that you might be particularly interested in:

- `spark.master` - (default: `local[*]`) Address of the master node of an 
  [Apache Spark](https://spark.apache.org/) cluster to use for processing data, 
  see [Master URLs](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls).
- `spark.executor.memory` - (default: `1g`) The quantity of memory available
  for each child task to process data within, in the same format as JVM memory
  strings with a size unit suffix (`k`, `m`, `g` or `t`) (e.g. `512m`, `2g`).
- `spark.sql.shuffle.partitions` - (default: `2`) This option controls the 
  number of data partitions used to distribute data between child tasks. This 
  can be tuned to higher numbers for larger data sets. It also controls the 
  granularity of requests made to the configured terminology service.

### Terminology service

- `pathling.terminology.enabled` - (default: `true`) Enables use of terminology
  functions within queries.
- `pathling.terminology.serverUrl` - (default:
  `https://r4.ontoserver.csiro.au/fhir`) The endpoint of the
  [FHIR terminology service](https://hl7.org/fhir/R4/terminology-service.html)
  (R4) that the server can use to resolve terminology queries.
- `pathling.terminology.socketTimeout` - (default: `60000`) The maximum period
  (in milliseconds) that the server should wait for incoming data from the
  terminology service.

### Authorisation

- `pathling.auth.enabled` - (default: `false`) Enables
  [SMART](https://hl7.org/fhir/smart-app-launch/index.html) authorisation. If
  this option is set to `true`, the `pathling.auth.jwksUrl`,
  `pathling.auth.issuer` and `pathling.auth.audience` options must also be set.
- `pathling.auth.jwksUrl` - Provides the URL of a
  [JSON Web Key Set](https://tools.ietf.org/html/rfc7517) from which the signing
  key for incoming bearer tokens can be retrieved, e.g.
  `https://pathling.au.auth0.com/.well-known/jwks.json`.
- `pathling.auth.issuer` - Configures the issuing domain for bearer tokens,
  which will be checked against the claims within incoming bearer tokens, e.g.
  `https://pathling.au.auth0.com/`.
- `pathling.auth.audience` - Configures the audience for bearer tokens, which is
  the FHIR endpoint that tokens are intended to be authorised for, e.g.
  `https://pathling.csiro.au/fhir`.
- `pathling.auth.authorizeUrl` - Provides the URL which will be advertised as
  the [authorisation endpoint](https://tools.ietf.org/html/rfc6749#section-3.1),
  e.g. `https://pathling.au.auth0.com/oauth/authorize`.
- `pathling.auth.tokenUrl` - Provides the URL which will be advertised as the
  [token endpoint](https://tools.ietf.org/html/rfc6749#section-3.2), e.g.
  `https://pathling.au.auth0.com/oauth/token`.
- `pathling.auth.revokeUrl` - Provides the URL which will be advertised as the
  [token revocation endpoint](https://tools.ietf.org/html/rfc7009), e.g.
  `https://pathling.au.auth0.com/oauth/revoke`.
  
### Caching

- `pathling.caching.enabled` - (default: `true`) Controls whether request 
  caching is enabled.
- `pathling.caching.aggregateRequestCacheSize` - (default: `100`) Controls the 
  maximum number of cache entries held in memory for the aggregate operation.
- `pathling.caching.searchBundleCacheSize` - (default: `100`) Controls the 
  maximum number of cache entries held in memory for the search operation.
- `pathling.caching.searchPageCacheSize` - (default: `100`) Controls the 
  maximum number of pages held in memory for each search operation.
- `pathling.caching.resourceReaderCacheSize` - (default: `100`) Controls the 
  maximum number of resource tables held in memory.

### Cross-Origin Resource Sharing (CORS)

See the 
[Cross-Origin Resource Sharing](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS)
specification for more information about the meaning of the different headers 
that are controlled by this configuration.

- `pathling.cors.allowedOrigins` - (default: `*`) This is a comma-delimited
  list of domain names that controls which domains are permitted to access the
  server per the `Access-Control-Allow-Origin` header.
- `pathling.cors.allowedMethods` - (default: `GET,POST`) This is a 
  comma-delimited list of HTTP methods permitted via the 
  `Access-Control-Allow-Methods` header.
- `pathling.cors.allowedHeaders` - (default: `Content-Type`) This is a 
  comma-delimited list of HTTP headers permitted via the 
  `Access-Control-Allow-Headers` header.
- `pathling.cors.maxAge` - (default: `600`) Controls how long the results of a 
  preflight request can be cached via the `Access-Control-Max-Age` header.

### Monitoring

- `pathling.sentryDsn` - If this variable is set, all errors will be reported to a
  [Sentry](https://sentry.io) service, e.g. `https://abc123@sentry.io/123456`.
- `pathling.sentryEnvironment` - If this variable is set, this will be sent as
  the environment when reporting errors to Sentry.

## Server base

There are a number of operations within the Pathling FHIR API that pass back
URLs referring back to API endpoints. The host and protocol components of these
URLs are automatically detected based upon the details of the incoming request.

In some cases it might be desirable to override the hostname and protocol,
particularly where Pathling is being hosted behind some sort of proxy. To
account for this, Pathling also supports the use of the
[X-Forwarded-Proto](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Proto),
[X-Forwarded-Host](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Host)
and `X-Forwarded-Port` headers to override the protocol, hostname and port 
within URLs sent back by the API.

## Authorisation

Pathling can perform the role of a resource server within the
[OAuth 2.0 framework](https://tools.ietf.org/html/rfc6749). The
[SMART App Launch Framework](https://hl7.org/fhir/smart-app-launch/index.html)
is a profile of OAuth 2.0 which is specific to the access of health data.

When authorisation is enabled through configuration, Pathling will refuse any
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
[Future work is planned](./roadmap.html#authorisation-enhancements) to expand
this to enable control over the access of individual resource types, aggregate
vs individual record data, and more.

## Apache Spark

Pathling can also be run directly within an Apache Spark cluster as a persistent
application.

For compatibility, Pathling runs Spark 3.1.1 (Scala 2.12), with Hadoop version
2.10.1.

Next: [Roadmap](./roadmap.html)
