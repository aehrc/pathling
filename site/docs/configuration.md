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
  **Important note**: a trailing slash should be used in cases where an attacker 
  could create an alternative URL with the same prefix, e.g. `s3://some-bucket` 
  would also match `s3://some-bucket-alternative`.

### Encoding

- `pathling.encoding.maxNestingLevel` - (default: `3`) Controls the maximum 
  depth of nested element data that is encoded upon import. This affects certain 
  elements within FHIR resources that contain recursive references, e.g. 
  [QuestionnaireResponse.item](https://hl7.org/fhir/R4/questionnaireresponse.html).

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

### Authorization

- `pathling.auth.enabled` - (default: `false`) Enables authorization. If
  this option is set to `true`, `pathling.auth.issuer` and 
  `pathling.auth.audience` options must also be set.
- `pathling.auth.issuer` - Configures the issuing domain for bearer tokens, e.g.
  `https://pathling.au.auth0.com/`. Must match the 
  contents of the [issuer claim](https://tools.ietf.org/html/rfc7519#section-4.1.1) 
  within bearer tokens.
- `pathling.auth.audience` - Configures the audience for bearer tokens, which is
  the FHIR endpoint that tokens are intended to be authorised for, e.g.
  `https://pathling.csiro.au/fhir`. Must match the contents of the 
  [audience claim](https://tools.ietf.org/html/rfc7519#section-4.1.3) 
  within bearer tokens.
  
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

- `pathling.cors.allowedOrigins` - (default: `[empty]`) This is a 
  comma-delimited list of domain names that controls which domains are permitted 
  to access the server per the `Access-Control-Allow-Origin` header. The value 
  `*` can be used with this parameter, but only when `pathling.auth.enabled` is 
  set to false. To use wildcards when authorization is enabled, please use 
  `pathling.cors.allowedOriginPatterns`.
- `pathling.cors.allowedOriginPatterns` - (default: `[empty]`) This is a 
  comma-delimited list of domain names that controls which domains are permitted
  to access the server per the `Access-Control-Allow-Origin` header. It differs 
  from `pathling.cors.allowedOrigins` in that it supports wildcard patterns, 
  e.g. `https://*.somedomain.com`.
- `pathling.cors.allowedMethods` - (default: `OPTIONS,GET,POST`) This is a 
  comma-delimited list of HTTP methods permitted via the 
  `Access-Control-Allow-Methods` header.
- `pathling.cors.allowedHeaders` - (default: `Content-Type,Authorization`) This 
  is a comma-delimited list of HTTP headers permitted via the 
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

## Authorization

Pathling can perform the role of a resource server within the
[OpenID Connect framework](https://openid.net/connect/).

When authorization is enabled through configuration, Pathling will refuse any
requests which are not accompanied by a valid
[bearer token](https://tools.ietf.org/html/rfc6750). The following requirements
must be met:

- Token is a [JSON Web Token](https://tools.ietf.org/html/rfc7519)
- Token contains an
  [audience claim](https://tools.ietf.org/html/rfc7519#section-4.1.3) that
  matches the configured value
- Token contains an [issuer claim](https://tools.ietf.org/html/rfc7519#section-4.1.1)
  that matches the configured value
- Issuer provides an [OpenID Connect Discovery endpoint](https://openid.net/specs/openid-connect-discovery-1_0.html) that provides information 
  about how to validate the token, including a link to a 
  [JSON Web Key Set](https://tools.ietf.org/html/rfc7517) containing the signing 
  key. This endpoint needs to be accessible to the Pathling server.
  
### Authorities

Pathling supports a set of authorities that control access to resources and 
operations. Authorities must be provided within the `authorities` claim within 
the JWT bearer token provided with each request.

<img src="/images/authorities.png"
srcset="/images/authorities@2x.png 2x, /images/authorities.png 1x"
alt="Authorities" />

| Authority                        | Description                                                                        |
| -------------------------------- | ---------------------------------------------------------------------------------- |
| `pathling`                       | Provides access to all operations and resources, implies all other authorities.    |
| `pathling:read`                  | Provides read access to all resource types.                                        |
| `pathling:read:[resource type]`  | Provides read access to only a specified resource type.                            |
| `pathling:write`                 | Provides write access to all resource types.                                       |
| `pathling:write:[resource type]` | Provides write access to only a specified resource type.                           |
| `pathling:import`                | Provides access to the import operation.                                           |
| `pathling:aggregate`             | Provides access to the aggregate operation.                                        |
| `pathling:search`                | Provides access to the search operation.                                           |

In order to enable access to an operation, an operation authority (e.g. 
`pathling:search`) must be provided along with a `read` or `write` authority 
(e.g. `pathling:read:Patient`).

Where expressions within a request reference multiple different resource types 
(e.g. through resource references), authority for read access to all those 
resources must be present within the token.

The import operation requires `write` authority for all resource types that are 
referenced within the request.

## Apache Spark

Pathling can also be run directly within an Apache Spark cluster as a persistent
application.

For compatibility, Pathling runs Spark 3.1.2 (Scala 2.12), with Hadoop version
2.10.1.

Next: [Roadmap](./roadmap.html)
