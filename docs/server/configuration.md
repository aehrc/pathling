# Configuration

Pathling is distributed in two forms: as a JAR file, and a Docker image. The easiest way to configure Pathling is through environment variables.

If environment variables are problematic for your deployment, Pathling can be also configured in a variety of other ways as supported by the Spring Boot framework (see [Spring Boot Reference Documentation: Externalized Configuration](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config)).

## Configuration variables[​](#configuration-variables "Direct link to Configuration variables")

### General[​](#general "Direct link to General")

* `server.port` - (default: `8080`) The port which the server should bind to and listen for HTTP connections.
* `server.servlet.context-path` - A prefix to add to the API endpoint, e.g. a value of `/foo` would cause the FHIR endpoint to be changed to `/foo/fhir`.
* `pathling.implementationDescription` - (default: `Yet Another Pathling Server`) Controls the content of the `implementation.description` element within the server's [CapabilityStatement](https://hl7.org/fhir/R4/http.html#capabilities).
* `JAVA_TOOL_OPTIONS` - (default in Docker image: `-Xmx2g`) Allows for the configuration of arbitrary options on the Java VM that Pathling runs within.

Additionally, you can set any variable supported by Spring Boot, see [Spring Boot Reference Documentation: Common Application properties](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#common-application-properties).

### Import[​](#import "Direct link to Import")

* `pathling.import.allowableSources` - (default: `file:///usr/share/staging`) A set of URL prefixes which are allowable for use within the import operation. **Important note**: a trailing slash should be used in cases where an attacker could create an alternative URL with the same prefix, e.g. `s3://some-bucket` would also match `s3://some-bucket-alternative`.

#### Ping and pull[​](#ping-and-pull "Direct link to Ping and pull")

These settings configure the [ping and pull import](/docs/server/operations/import-pnp.md) operation, which retrieves data from external FHIR bulk export endpoints.

* `pathling.import.pnp.clientId` - The client identifier for SMART Backend Services authentication.
* `pathling.import.pnp.tokenEndpoint` - The token endpoint URL for obtaining access tokens. If not specified, the client will attempt to discover it via the SMART configuration endpoint.
* `pathling.import.pnp.privateKeyJwk` - The private key in JWK format for asymmetric authentication (RS384).
* `pathling.import.pnp.clientSecret` - The client secret for symmetric authentication.
* `pathling.import.pnp.scope` - The requested scope for authentication (e.g., `system/*.read`).
* `pathling.import.pnp.tokenExpiryTolerance` - (default: `120`) The minimum number of seconds that a token should have before expiry when deciding whether to use it. If a cached token has less than this many seconds until expiry, a new token will be requested.
* `pathling.import.pnp.downloadLocation` - (default: `/usr/share/staging/pnp`) The directory where files will be downloaded during ping and pull import operations. This location should have sufficient space for large FHIR exports.
* `pathling.import.pnp.fileExtension` - (default: `.ndjson`) The file extension to filter for when processing downloaded files.

### Export[​](#export "Direct link to Export")

* `pathling.export.resultExpiry` - (default: `86400`) The duration in seconds that export results will be available before expiry. Defaults to 24 hours.

### Bulk submit[​](#bulk-submit "Direct link to Bulk submit")

* `pathling.bulkSubmit.allowedSubmitters` - (default: `[]`) The list of allowed submitters (by system and value) that can use the $bulk-submit operation.
* `pathling.bulkSubmit.allowableSources` - (default: `https://`) URL prefixes that are allowed as sources for manifest and file URLs.

### Asynchronous processing[​](#asynchronous-processing "Direct link to Asynchronous processing")

* `pathling.async.enabled` - (default: `true`) Enables asynchronous processing for those operations that support it, when explicitly requested.
* `pathling.async.varyHeadersExcludedFromCacheKey` - (default: `Accept`, `Accept-Encoding`) A subset of `pathling.httpCaching.vary` HTTP headers, which should be excluded from determining that asynchronous requests are equivalent and can be routed to the same asynchronous job.
* `pathling.async.cacheMaxAge` - (default: `1`) The max-age value (in seconds) for `Cache-Control` headers on async endpoint responses (`$job` and `$result`). This value should be configured to match your strategy for cleaning up job result files, such as an S3 bucket lifecycle policy. For example, if job files are retained for 24 hours, set this to `86400`.

### Operations[​](#operations "Direct link to Operations")

These settings enable or disable individual server operations. All operations are enabled by default. When an operation is disabled, it returns a client error response and is excluded from the CapabilityStatement.

* `pathling.operations.createEnabled` - (default: `true`) Enables CRUD create operations.
* `pathling.operations.readEnabled` - (default: `true`) Enables CRUD read operations.
* `pathling.operations.updateEnabled` - (default: `true`) Enables CRUD update operations.
* `pathling.operations.deleteEnabled` - (default: `true`) Enables CRUD delete operations.
* `pathling.operations.searchEnabled` - (default: `true`) Enables CRUD search operations.
* `pathling.operations.batchEnabled` - (default: `true`) Enables batch/transaction bundle operations.
* `pathling.operations.exportEnabled` - (default: `true`) Enables the system-level [$export](/docs/server/operations/export.md) operation.
* `pathling.operations.patientExportEnabled` - (default: `true`) Enables the Patient-level [$export](/docs/server/operations/export.md) operation.
* `pathling.operations.groupExportEnabled` - (default: `true`) Enables the Group-level [$export](/docs/server/operations/export.md) operation.
* `pathling.operations.importEnabled` - (default: `true`) Enables the [$import](/docs/server/operations/import.md) operation.
* `pathling.operations.importPnpEnabled` - (default: `true`) Enables the [$import-pnp](/docs/server/operations/import-pnp.md) operation.
* `pathling.operations.viewDefinitionRunEnabled` - (default: `true`) Enables the system-level [$viewdefinition-run](/docs/server/operations/view-run.md) operation.
* `pathling.operations.viewDefinitionInstanceRunEnabled` - (default: `true`) Enables the instance-level [$run](/docs/server/operations/view-run.md) operation on ViewDefinition resources.
* `pathling.operations.viewDefinitionExportEnabled` - (default: `true`) Enables the [$viewdefinition-export](/docs/server/operations/view-export.md) operation.
* `pathling.operations.bulkSubmitEnabled` - (default: `true`) Enables the [$bulk-submit](/docs/server/operations/bulk-submit.md) operation.

### Encoding[​](#encoding "Direct link to Encoding")

* `pathling.encoding.maxNestingLevel` - (default: `3`) Controls the maximum depth of nested element data that is encoded upon import. This affects certain elements within FHIR resources that contain recursive references, e.g. [QuestionnaireResponse.item](https://hl7.org/fhir/R4/questionnaireresponse.html).
* `pathling.encoding.enableExtensions` - (default: `true`) Enables support for FHIR extensions.
* `pathling.encoding.openTypes` - (default: `boolean`, `code`, `date`, `dateTime`, `decimal`, `integer`, `string`, `Coding`, `CodeableConcept`, `Address`, `Identifier`, `Reference`) The list of types that are encoded within open types, such as extensions. This default list was taken from the data types that are common to extensions found in widely-used IGs, such as the US and AU base profiles. In general, you will get the best query performance by encoding your data with the shortest possible list.

### Storage[​](#storage "Direct link to Storage")

* `pathling.storage.warehouseUrl` - (default: `file:///usr/share/warehouse`) The base URL at which Pathling will look for data files, and where it will save data received within [import](/docs/server/operations/import.md) requests. Can be an [Amazon S3](https://aws.amazon.com/s3/) (`s3a://`), [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (`hdfs://`) or filesystem (`file://`) URL.
* `pathling.storage.databaseName` - (default: `default`) The subdirectory within the warehouse path used to read and write data.
* `pathling.storage.cacheDatasets` - (default: `true`) This controls whether the built-in caching within Spark is used for resource datasets. It may be useful to turn this off for large datasets in memory-constrained environments.
* `pathling.storage.compactionThreshold` - (default: `10`) When a table is updated, the number of partitions is checked. If the number exceeds this threshold, the table will be repartitioned back to the default number of partitions. This prevents large numbers of small updates causing poor subsequent query performance.

Pathling will automatically detect AWS authentication details within the environment and use them to access S3 buckets. It uses a chain of authentication methods, see [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) for details.

In addition to this, any Hadoop S3 configuration variable (`fs.s3a.*`) can be set within Pathling directly. See the [Hadoop AWS documentation](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html) for all the possible options.

This is the default S3 configuration, along with some hints on how to add some common configuration parameters:

```
fs:
    s3a:
        aws:
            # credentials:
            #   provider: org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
            #   provider: org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
            #
            # For use with: SimpleAWSCredentialsProvider
            # access:
            #   key: [access key]
            # secret:
            #   key: [secret key]
            #
            # For use with: AssumedRoleCredentialProvider
            # assumed:
            #   role:
            #     arn: [role ARN]
            #
            connection:
                maximum: 100
            committer:
                name: magic
                magic:
                    enabled: true
```

### Query[​](#query "Direct link to Query")

* `pathling.query.explainQueries` - (default: `false`) If set to true, Spark query plans will be written to the logs.
* `pathling.query.cacheResults` - (default: `true`) This controls whether the built-in caching within Spark is used for search results. It may be useful to turn this off for large datasets in memory-constrained environments.

### Apache Spark[​](#apache-spark "Direct link to Apache Spark")

* `pathling.spark.appName` - (default: `pathling`) Controls the application name that Pathling will be identified as within any Spark cluster that it participates in.

Any Spark configuration variable can be set within Pathling directly. See [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) for the full list.

Here are a few that you might be particularly interested in:

* `spark.master` - (default: `local[*]`) Address of the master node of an [Apache Spark](https://spark.apache.org/) cluster to use for processing data, see [Master URLs](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls).
* `spark.executor.memory` - (default: `1g`) The quantity of memory available for each child task to process data within, in the same format as JVM memory strings with a size unit suffix (`k`, `m`, `g` or `t`) (e.g. `512m`, `2g`).
* `spark.sql.shuffle.partitions` - (default: `2`) This option controls the number of data partitions used to distribute data between child tasks. This can be tuned to higher numbers for larger data sets. It also controls the granularity of requests made to the configured terminology service.

This is the default Spark configuration:

```
spark:
    master: local[*]
    sql:
        adaptive:
            enabled: true
            coalescePartitions:
                enabled: true
        extensions: io.delta.sql.DeltaSparkSessionExtension
        catalog:
            spark_catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog
    databricks:
        delta:
            schema:
                autoMerge:
                    enabled: false
    scheduler:
        mode: FAIR
```

### Terminology service[​](#terminology-service "Direct link to Terminology service")

* `pathling.terminology.enabled` - (default: `true`) Enables use of terminology functions within queries.
* `pathling.terminology.serverUrl` - (default: `https://tx.ontoserver.csiro.au/fhir`) The endpoint of the [FHIR terminology service](https://hl7.org/fhir/R4/terminology-service.html) (R4) that the server can use to resolve terminology queries.
* `pathling.terminology.verboseLogging` - (default: `false`) Setting this option to `true` will enable additional logging of the details of requests between the server and the terminology service.
* `pathling.terminology.acceptLanguage` - If this variable is set, it will be used as the value of the `Accept-Language` HTTP header passed to the terminology server. The value may contain multiple languages, with weighted preferences as defined in [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-accept-language). If not provided, the header is not sent. The server can use the header to return the result in the preferred language if it is able. The actual behaviour may depend on the server implementation and the code systems used.

#### Client[​](#client "Direct link to Client")

* `pathling.terminology.client.maxConnectionsTotal` - (default: `32`) The maximum number of total connections allowed from the client.
* `pathling.terminology.client.maxConnectionsPerRoute` - (default: `16`) The maximum number of connections allowed from the client, per route.
* `pathling.terminology.client.socketTimeout` - (default: `60000`) The maximum period (in milliseconds) that the server should wait for incoming data from the terminology service.
* `pathling.terminology.client.retryEnabled` - (default: `true`) Enables automatic retry of failed terminology service requests.
* `pathling.terminology.client.retryCount` - (default: `2`) The maximum number of times that failed terminology service requests should be retried.

#### Cache[​](#cache "Direct link to Cache")

* `pathling.terminology.cache.enabled` - (default: `true`) Set this to false to disable caching of terminology requests (not recommended).
* `pathling.terminology.cache.storageType` - (default: `memory`) The type of storage to be used by the terminology cache. Valid values are `memory` and `disk`.
* `pathling.terminology.cache.maxEntries` - (default: `50000`) Sets the maximum number of entries that will be held in memory. Only applicable when using the `memory` storage type.
* `pathling.terminology.cache.storagePath` - The path at which to store cache data. Required if `pathling.terminology.cache.storageType` is set to `disk`.
* `pathling.terminology.cache.defaultExpiry` - (default: `600`) The amount of time (in seconds) that a response from the terminology server should be cached if the server does not specify an expiry.
* `pathling.terminology.cache.overrideExpiry` - If provided, this value overrides the expiry time provided by the terminology server.

#### Authentication[​](#authentication "Direct link to Authentication")

* `pathling.terminology.authentication.enabled` - (default: `false`) Enables authentication for requests to the terminology service.
* `pathling.terminology.authentication.tokenEndpoint`, `pathling.terminology.authentication.clientId`, `pathling.terminology.authentication.clientSecret` - Authentication details for connecting to a terminology service that requires authentication, using [OAuth 2.0 client credentials flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4).

### Authorization[​](#authorization "Direct link to Authorization")

* `pathling.auth.enabled` - (default: `false`) Enables authorization. If this option is set to `true`, `pathling.auth.issuer` and `pathling.auth.audience` options must also be set.
* `pathling.auth.issuer` - Configures the issuing domain for bearer tokens, e.g. `https://pathling.au.auth0.com/`. Must match the contents of the [issuer claim](https://tools.ietf.org/html/rfc7519#section-4.1.1) within bearer tokens.
* `pathling.auth.audience` - Configures the audience for bearer tokens, which is the FHIR endpoint that tokens are intended to be authorised for, e.g. `https://pathling.csiro.au/fhir`. Must match the contents of the [audience claim](https://tools.ietf.org/html/rfc7519#section-4.1.3) within bearer tokens.
* `pathling.auth.capabilities` - (default: `launch-standalone`) A list of [SMART on FHIR capabilities](https://hl7.org/fhir/smart-app-launch/conformance.html) to advertise in the SMART configuration document at `/.well-known/smart-configuration`.
* `pathling.auth.grantTypesSupported` - (default: `authorization_code`) A list of OAuth 2.0 grant types supported at the token endpoint, e.g. `authorization_code`, `client_credentials`, `refresh_token`. See [SMART App Launch Conformance](https://hl7.org/fhir/smart-app-launch/conformance.html) for more details.
* `pathling.auth.codeChallengeMethodsSupported` - (default: `S256`) A list of PKCE code challenge methods supported. Must include `S256` and must not include `plain` as per the SMART specification.

### Admin UI[​](#admin-ui "Direct link to Admin UI")

* `pathling.adminUi.clientId` - The OAuth client ID for the admin UI. When set, this value is included in the SMART configuration response at `/.well-known/smart-configuration` as `admin_ui_client_id`. The admin UI uses this value when initiating OAuth flows. If not set, the UI falls back to the `VITE_CLIENT_ID` environment variable (set at build time), then to the default value `pathling-admin-ui`. See [Admin UI](/docs/server/admin-ui.md) for more details.

### HTTP caching[​](#http-caching "Direct link to HTTP caching")

* `pathling.httpCaching.vary` - (default: `Accept`, `Accept-Encoding`, `Prefer`, `Authorization`) A list of values to return within the `Vary` header.
* `pathling.httpCaching.cacheableControl` - (default: `must-revalidate`, `max-age=1`) A list of values to return within the `Cache-Control` header, for cacheable responses.
* `pathling.httpCaching.uncacheableControl` - (default: `no-store`) A list of values to return within the `Cache-Control` header, for uncacheable responses.

### Cross-Origin Resource Sharing (CORS)[​](#cross-origin-resource-sharing-cors "Direct link to Cross-Origin Resource Sharing (CORS)")

See the [Cross-Origin Resource Sharing](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS) specification for more information about the meaning of the different headers that are controlled by this configuration.

* `pathling.cors.allowedOriginPatterns` - (default: `[empty]`) This is a comma-delimited list of domain name patterns that controls which domains are permitted to access the server per the `Access-Control-Allow-Origin` header. Supports wildcard patterns, e.g. `https://*.somedomain.com`.
* `pathling.cors.allowedMethods` - (default: `OPTIONS,GET,POST`) This is a comma-delimited list of HTTP methods permitted via the `Access-Control-Allow-Methods` header.
* `pathling.cors.allowedHeaders` - (default: `Content-Type,Authorization,Prefer`) This is a comma-delimited list of HTTP headers permitted via the `Access-Control-Allow-Headers` header.
* `pathling.cors.exposedHeaders` - (default: `Content-Location,Content-Disposition,X-Progress`) This is a comma-delimited list of HTTP headers that are permitted to be exposed via the `Access-Control-Expose-Headers` header.
* `pathling.cors.maxAge` - (default: `600`) Controls how long the results of a preflight request can be cached via the `Access-Control-Max-Age` header.

### Monitoring[​](#monitoring "Direct link to Monitoring")

* `pathling.sentryDsn` - If this variable is set, all errors will be reported to a [Sentry](https://sentry.io) service, e.g. `https://abc123@sentry.io/123456`.
* `pathling.sentryEnvironment` - If this variable is set, this will be sent as the environment when reporting errors to Sentry.

## Server base[​](#server-base "Direct link to Server base")

There are a number of operations within the Pathling FHIR API that pass back URLs referring back to API endpoints. The host and protocol components of these URLs are automatically detected based upon the details of the incoming request.

In some cases it might be desirable to override the hostname and protocol, particularly where Pathling is being hosted behind some sort of proxy. To account for this, Pathling also supports the use of the [X-Forwarded-Proto](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Proto), [X-Forwarded-Host](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Host) and `X-Forwarded-Port` headers to override the protocol, hostname and port within URLs sent back by the API.
