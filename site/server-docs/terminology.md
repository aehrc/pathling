---
sidebar_position: 4.5
description: How to configure Pathling Server to use a remote FHIR terminology server, with or without authentication, or a local terminology store built with the Pathling libraries.
---

# Terminology

The server answers terminology questions in two places:

- the FHIRPath terminology functions (`memberOf`, `translate`, `subsumes`,
  `subsumedBy`, `display`, `property` and `designation`), used in
  ViewDefinitions and [FHIRPath search](/docs/server/operations/search); and
- the value set and concept map dependencies of SQLQueries and SQLViews (see
  [Terminology in SQL](./sql-on-fhir.md#terminology-in-sql)).

Both are served by one terminology layer, which works in one of two modes,
selected by `pathling.terminology.mode`:

| Mode               | Source of terminology                                                           | Network dependency                     |
| ------------------ | ------------------------------------------------------------------------------- | -------------------------------------- |
| `server` (default) | A remote [FHIR terminology server](#using-a-remote-terminology-server)          | Every node must reach the server       |
| `local`            | A [local terminology store](#using-a-local-terminology-store), built in advance | None, beyond access to the store files |

The complete list of settings is in the
[configuration reference](./configuration.md#terminology-service).

## Using a remote terminology server

In `server` mode, terminology requests are sent to
`pathling.terminology.serverUrl`. Any FHIR R4 terminology server that implements
`$validate-code`, `$translate`, `$subsumes`, `$lookup` and `ValueSet/$expand`
can be used, and SQL concept map dependencies additionally require ConceptMap
search and read. Pathling is developed and tested against
[Ontoserver](https://ontoserver.csiro.au/).

The default is the public Ontoserver instance at
`https://tx.ontoserver.csiro.au/fhir`, which is suitable for testing only.

Requests are made from the Spark executors as well as the server process, so in
a [cluster deployment](./deployment/kubernetes.md#cluster) the terminology
server must be reachable from every executor pod. If it presents a certificate
issued by a private certificate authority, mount a
[custom trust store](./deployment/kubernetes.md#custom-trust-store).

### Without authentication

Set the server URL:

```bash
PATHLING_TERMINOLOGY_SERVERURL=https://tx.example.org/fhir
```

The same configuration in the `pathling.config` map of the
[Helm chart](./deployment/kubernetes.md):

```yaml
pathling:
    config:
        pathling.terminology.serverUrl: https://tx.example.org/fhir
```

Responses are cached on each Spark executor, honouring the expiry the terminology server
sends. The cache, connection pool, timeouts and retries can be tuned with the
[client](./configuration.md#client) and [cache](./configuration.md#cache)
settings. `pathling.terminology.acceptLanguage` sets the `Accept-Language`
header sent with each request, for servers that can return displays in more
than one language.

### With authentication

Pathling authenticates to a protected terminology server using the
[OAuth 2.0 client credentials grant](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4).
It obtains a token from the token endpoint, sends it as a bearer token with each
request, and refreshes it before it expires.

This example authenticates to the
[NHS England terminology server](https://ontology.nhs.uk/) with a client
secret:

```bash
PATHLING_TERMINOLOGY_SERVERURL=https://ontology.nhs.uk/production1/fhir
PATHLING_TERMINOLOGY_AUTHENTICATION_ENABLED=true
PATHLING_TERMINOLOGY_AUTHENTICATION_TOKENENDPOINT=https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token
PATHLING_TERMINOLOGY_AUTHENTICATION_CLIENTID=my-client-id
PATHLING_TERMINOLOGY_AUTHENTICATION_CLIENTSECRET=my-client-secret
```

With the Helm chart, put the client secret in `pathling.secretConfig` so that it
is stored in a Kubernetes secret rather than in the deployment:

```yaml
pathling:
    config:
        pathling.terminology.serverUrl: https://ontology.nhs.uk/production1/fhir
        pathling.terminology.authentication.enabled: "true"
        pathling.terminology.authentication.tokenEndpoint: https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token
        pathling.terminology.authentication.clientId: my-client-id
    secretConfig:
        pathling.terminology.authentication.clientSecret: my-client-secret
```

The authentication settings, all under `pathling.terminology.authentication`:

- `enabled` - (default: `false`) Enables authentication. It must be set
  alongside the credentials.
- `tokenEndpoint` - The OAuth 2.0 token endpoint.
- `clientId` - The client ID.
- `clientSecret` - The client secret, for symmetric authentication.
- `privateKeyJwk` - A private key in JWK format, for asymmetric authentication
  with a signed JWT
  ([RFC 7523](https://datatracker.ietf.org/doc/html/rfc7523)). Use instead of
  `clientSecret`; if both are set, the private key is used.
- `useFormForBasicAuth` - (default: `false`) Sends the client ID and secret in
  the body of the token request rather than in an HTTP Basic `Authorization`
  header. Some token endpoints accept only one of the two.
- `scope` - A scope to request with the token.
- `tokenExpiryTolerance` - (default: `120`) The minimum number of seconds a
  token must have before expiry for it to be used; a token closer to expiry
  than this is refreshed first.

The server will not start with authentication enabled unless `tokenEndpoint`,
`clientId` and either `clientSecret` or `privateKeyJwk` are set.

## Using a local terminology store

In `local` mode, terminology is evaluated against a store of SNOMED CT and FHIR
terminology content, with no requests to a terminology server. This removes the
network dependency, and the request volume and latency that come with it, and
pins results to the content in the store.

The server reads the store but does not build it. Build it beforehand with the
Pathling libraries, then point the server at it.

### Building the store

The store is a set of Delta tables, written to any location reachable through
the Hadoop FileSystem API: a local directory, a mounted volume, or an object
store such as S3. Content is imported with the
[Pathling CLI](/docs/libraries/cli#terminology-import-commands), which needs
only [uv](https://docs.astral.sh/uv/) and a Java runtime:

```bash
# SNOMED CT, from an RF2 snapshot release (a .zip archive or an extracted
# directory). This example imports the Australian edition, using Australian
# English preferred terms as the stored display.
uvx pathling import-snomed --default-dialect en-AU \
  /data/NCTS_SCT_RF2_DISTRIBUTION_32506021000036107-20260930-ALL.zip \
  /data/tx-store

# FHIR CodeSystems, ValueSets and ConceptMaps, from a FHIR NPM package (.tgz),
# a JSON file or a directory of JSON files. This example imports HL7
# Terminology (THO).
curl -L -o /data/hl7.terminology.r4-7.4.0.tgz \
  https://packages.simplifier.net/hl7.terminology.r4/7.4.0
uvx pathling import-fhir-terminology /data/hl7.terminology.r4-7.4.0.tgz \
  /data/tx-store
```

Each command adds to the store, so run them once for each source. Re-importing
a version that the store already holds replaces it atomically.

The same imports are available as functions of the
[Python and R libraries](/docs/libraries/terminology/local#importing-content),
for example within a Databricks job that writes the store to object storage:

```python
from pathling import PathlingContext

pc = PathlingContext.create()
pc.import_snomed("/data/rf2.zip", "s3a://my-bucket/tx-store")
pc.import_fhir_terminology("/data/hl7.terminology.tgz", "s3a://my-bucket/tx-store")
```

Things to know before importing:

- **An RF2 source must be self-contained.** An extension or derived package
  imported without the edition it depends on silently loses most of its
  content. See
  [RF2 sources must be self-contained](/docs/libraries/terminology/local#rf2-sources-must-be-self-contained).
- **A release with several language reference sets needs a dialect.** Other
  than the International edition, such a release fails the import until one is
  named with `--default-dialect`. See
  [dialects](/docs/libraries/terminology/local#dialects).
- **Large vocabularies need more driver memory.** Raise it with the
  `SPARK_DRIVER_MEMORY` environment variable, e.g. `SPARK_DRIVER_MEMORY=4g`.
  Progress is logged when `--verbose` is passed.
- **The store format must be one the server can read.** A store records the
  format version it was written in, and a server that supports only an earlier
  format refuses to start, naming both versions. A store built with a newer
  release of the library than the one the server is built on may therefore need
  rebuilding with an older release.

### Configuring the server

Set the mode and the location of the store:

```bash
PATHLING_TERMINOLOGY_MODE=local
PATHLING_TERMINOLOGY_LOCAL_STORAGEPATH=file:///usr/share/terminology
```

With Docker Compose, mount the store built above into the container:

```yaml
services:
    pathling:
        image: ghcr.io/aehrc/pathling:latest
        ports:
            - "8080:8080"
        volumes:
            - pathling-data:/usr/share/warehouse
            - /data/tx-store:/usr/share/terminology:ro
        environment:
            - pathling.storage.warehouseUrl=file:///usr/share/warehouse
            - pathling.terminology.mode=local
            - pathling.terminology.local.storagePath=file:///usr/share/terminology

volumes:
    pathling-data:
```

With the Helm chart, a store in object storage needs no volume. The S3A
credentials are Spark configuration, so the same settings that give the server
access to an S3 warehouse also give it access to the store (see
[SeaweedFS object storage](./deployment/kubernetes.md#seaweedfs-object-storage)):

```yaml
pathling:
    config:
        pathling.terminology.mode: local
        pathling.terminology.local.storagePath: s3a://my-bucket/tx-store
```

The store is opened when the server starts, so a missing `storagePath`, or a
store that cannot be read, stops the server from starting. In a cluster
deployment the executors also open the store, so the path must be reachable from
every executor pod: an object store bucket, or a volume mounted into the
executor pods as well as the server pod.

The other local mode settings:

- `pathling.terminology.local.defaultSnomedEdition` - The module identifier of
  the SNOMED CT edition used for an unversioned SNOMED CT reference, required
  when the store holds more than one edition (e.g. `32506021000036107` for the
  Australian edition).
- `pathling.terminology.local.expansionCacheSize` - (default: `100`) The
  maximum number of value set expansions held in memory by each Spark executor.
- `pathling.terminology.local.dialectAliases.<tag>` - Registers an additional
  language tag for the SNOMED CT language reference set that serves it, e.g.
  `pathling.terminology.local.dialectAliases.en-NZ=271000210107`.

The `serverUrl`, `client`, `cache` and `authentication` settings have no effect
in `local` mode, and the server logs a warning at startup for each one that has
been changed from its default. In `local` mode, SQL dependencies on SNOMED CT
implicit concept maps (`http://snomed.info/sct?fhir_cm=...`) are synthesised
from the store; see [Terminology in SQL](./sql-on-fhir.md#where-terminology-comes-from).

## Disabling terminology

Set `pathling.terminology.enabled` to `false` to run without a terminology
layer. The terminology functions are then unavailable, and a SQL value set or
concept map dependency resolves only if it is supplied inline in the request's
`context`.
