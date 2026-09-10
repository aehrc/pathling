---
sidebar_position: 1
description: How to point the Pathling library at a FHIR terminology server, tune the HTTP client and cache, and authenticate to a protected server.
---

# Terminology server support

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

By default, the [terminology functions](./index.md) are evaluated by sending requests to
a [FHIR terminology server](https://hl7.org/fhir/R4/terminology-service.html).
Any server that implements the R4 terminology operations (`$validate-code`,
`$translate`, `$subsumes` and `$lookup`) can be used. Pathling is developed and
tested against [Ontoserver](https://ontoserver.csiro.au/), and the default
server is a public Ontoserver instance at `https://tx.ontoserver.csiro.au/fhir`.
The default server is suitable for testing purposes only. Point Pathling at your
own server for anything beyond that.

Terminology requests are made from the Spark executors, so the server must be
reachable from every node in the cluster, not just the driver.

If you would rather not depend on a server at all, see
[local terminology mode](./local.md).

## Configuring the server

The server URL and related settings are passed when the context is created.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create(
        terminology_server_url="https://tx.example.org/fhir"
)
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect(
        terminology_server_url = "https://tx.example.org/fhir"
)
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.config.TerminologyConfiguration
import au.csiro.pathling.library.PathlingContext

val terminologyConfig = TerminologyConfiguration.builder()
        .serverUrl("https://tx.example.org/fhir")
        .build()
val pc = PathlingContext.builder()
        .terminologyConfiguration(terminologyConfig)
        .build()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.library.PathlingContext;

class MyApp {

    public static void main(String[] args) {
        TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder()
                .serverUrl("https://tx.example.org/fhir")
                .build();
        PathlingContext pc = PathlingContext.builder()
                .terminologyConfiguration(terminologyConfig)
                .build();
        // ...
    }
}
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling --tx-server https://tx.example.org/fhir member-of codes.csv \
  --code-column code --system 'http://snomed.info/sct' \
  --value-set 'http://snomed.info/sct?fhir_vs=ecl/<< 73211009'
```

The server can also be recorded once as `tx-server` in the configuration file.
See the [command line interface documentation](../cli#global-options) for
details.

</TabItem>
</Tabs>

The following parameters control the connection to the server. The Python and R
names are given first, with the corresponding Java configuration in brackets.

- `terminology_server_url` (`TerminologyConfiguration.serverUrl`): the endpoint
  of the FHIR R4 terminology server.
- `terminology_verbose_request_logging`
  (`TerminologyConfiguration.verboseLogging`): logs the details of each request
  at the `DEBUG` level. Logging is subject to the Spark logging level, which you
  can set using `SparkContext.setLogLevel`.
- `accept_language` (`TerminologyConfiguration.acceptLanguage`): the default
  value of the `Accept-Language` header sent with each request. See
  [multi-language support](./index.md#multi-language-support).
- `terminology_socket_timeout` (`HttpClientConfiguration.socketTimeout`): the
  maximum period in milliseconds to wait for data from the server. Defaults to
  60,000.
- `max_connections_total` and `max_connections_per_route`
  (`HttpClientConfiguration.maxConnectionsTotal` and `maxConnectionsPerRoute`):
  the size of the HTTP connection pool on each executor. Default to 32 and 16.
- `terminology_retry_enabled` and `terminology_retry_count`
  (`HttpClientConfiguration.retryEnabled` and `retryCount`): whether requests
  that fail for possibly transient reasons, such as network or DNS problems, are
  retried, and how many times. Default to enabled and 2.

## Caching

Responses from the server are cached on each executor, so that repeated
requests for the same code do not result in repeated round trips. Caching is
enabled by default and honours the expiry information the server sends in its
response headers. It is not recommended to disable it.

- `enable_cache` (`HttpClientCachingConfiguration.enabled`): whether responses
  are cached.
- `cache_max_entries` (`HttpClientCachingConfiguration.maxEntries`): the maximum
  number of entries held in the cache. Defaults to 200,000.
- `cache_storage_type` (`HttpClientCachingConfiguration.storageType`): `memory`
  (the default), which is reset when the application restarts, or `disk`, which
  is persisted between restarts.
- `cache_storage_path` (`HttpClientCachingConfiguration.storagePath`): the
  directory used for the cache, required when the storage type is `disk`.
- `cache_default_expiry` (`HttpClientCachingConfiguration.defaultExpiry`): the
  expiry time in seconds used when the server does not provide one. Defaults to 600.
- `cache_override_expiry` (`HttpClientCachingConfiguration.overrideExpiry`): if
  set, an expiry time in seconds that overrides whatever the server provides.

A disk cache is useful when the same codes are looked up across many sessions,
for example in a scheduled job. Set `cache_storage_path` to a location that is
local to each executor.

## Authentication

Pathling can be configured to connect to a protected terminology server by
supplying a set of OAuth2 client credentials and a token endpoint. Tokens are
obtained using the client credentials grant and refreshed automatically before
they expire.

Here is an example of how to authenticate to
the [NHS terminology server](https://ontology.nhs.uk/):

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create(
        terminology_server_url='https://ontology.nhs.uk/production1/fhir',
        enable_auth=True,
        token_endpoint='https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token',
        client_id='[client ID]',
        client_secret='[client secret]'
)
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect(
        terminology_server_url = "https://ontology.nhs.uk/production1/fhir",
        enable_auth = TRUE,
        token_endpoint = "https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token",
        client_id = "[client ID]",
        client_secret = "[client secret]"
)
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.config.{TerminologyAuthConfiguration, TerminologyConfiguration}
import au.csiro.pathling.library.PathlingContext

val authConfig = TerminologyAuthConfiguration.builder()
        .enabled(true)
        .tokenEndpoint("https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token")
        .clientId("[client ID]")
        .clientSecret("[client secret]")
        .build()
val terminologyConfig = TerminologyConfiguration.builder()
        .serverUrl("https://ontology.nhs.uk/production1/fhir")
        .authentication(authConfig)
        .build()
val pc = PathlingContext.builder()
        .terminologyConfiguration(terminologyConfig)
        .build()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.library.PathlingContext;

class MyApp {

    public static void main(String[] args) {
        TerminologyAuthConfiguration authConfig = TerminologyAuthConfiguration.builder()
                .enabled(true)
                .tokenEndpoint(
                        "https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token")
                .clientId("[client ID]")
                .clientSecret("[client secret]")
                .build();
        TerminologyConfiguration terminologyConfig = TerminologyConfiguration.builder()
                .serverUrl("https://ontology.nhs.uk/production1/fhir")
                .authentication(authConfig)
                .build();
        PathlingContext pc = PathlingContext.builder()
                .terminologyConfiguration(terminologyConfig)
                .build();
        // ...
    }
}
```

</TabItem>
<TabItem value="cli" label="CLI">

```bash
pathling --tx-server https://ontology.nhs.uk/production1/fhir \
  --tx-token-endpoint https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token \
  --tx-client-id '[client ID]' --tx-client-secret '[client secret]' \
  display codes.csv --code-column code --system 'http://snomed.info/sct'
```

The credentials can also be recorded in the `[terminology-auth]` table of the
configuration file. See the
[command line interface documentation](../cli#configuration-file) for details.

</TabItem>
</Tabs>

The following parameters control authentication:

- `enable_auth` (`TerminologyAuthConfiguration.enabled`): enables
  authentication. Off by default, so it must be set alongside the credentials.
- `token_endpoint` (`TerminologyAuthConfiguration.tokenEndpoint`): the OAuth2
  token endpoint.
- `client_id` and `client_secret` (`TerminologyAuthConfiguration.clientId` and
  `clientSecret`): the client credentials.
- `scope` (`TerminologyAuthConfiguration.scope`): an optional scope value to
  request with the token.
- `token_expiry_tolerance` (`TerminologyAuthConfiguration.tokenExpiryTolerance`):
  the minimum number of seconds a token must have before expiry for it to be
  sent with a request; a token closer to expiry than this is refreshed first.
  Defaults to 120.

Avoid embedding the client secret in source code. Read it from an environment
variable or a secrets manager at runtime.
