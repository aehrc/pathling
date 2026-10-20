# Configuration

Defaults for the [global options](/docs/libraries/cli.md#global-options) and a few command-specific settings can be recorded in a [TOML](https://toml.io/) file, so that they need not be passed on every invocation. Values are resolved with the precedence flag > config file > built-in default.

## Config file location[​](#config-file-location "Direct link to Config file location")

Exactly one config file is ever read, chosen by this precedence (highest first):

1. The path given to `--config`.
2. A `pathling.toml` in the current working directory.
3. The user-level `${XDG_CONFIG_HOME:-~/.config}/pathling/config.toml`.
4. None, in which case built-in defaults apply.

Because files are never merged, any key the chosen file omits falls back to its built-in default rather than to a value from another file. Discovery is limited to the current working directory; the CLI does not search parent directories.

A project-local `pathling.toml` lets a project carry its own settings (for example a particular terminology server) without editing your personal config or passing `--config` on every command. When one is discovered and used, the CLI prints a one-line notice on standard error naming the file (and the user-level file it overrides, when one exists), so the active configuration is never a surprise:

```
Using project config /path/to/pathling.toml (overrides ~/.config/pathling/config.toml).
```

Passing an explicit `--config` skips project-local discovery, and no such notice is printed. An explicit `--config` path must exist; a missing path is an error rather than a silent fall-back to another config file.

## Example[​](#example "Direct link to Example")

```
tx-server = "https://tx.example.org/fhir"
fhir-version = "R4"
package-registry = "https://packages.simplifier.net"

[terminology-auth]
client-id = "my-client"
client-secret = "@/run/secrets/tx-secret"
token-endpoint = "https://auth.example.org/token"

[bulk-auth]
client-id = "bulk-client"
private-key-jwk = "@/run/secrets/bulk-key.jwk"
token-endpoint = "https://auth.example.org/token"
scope = "system/*.read"

[spark]
"spark.sql.shuffle.partitions" = 16
"spark.executor.memory" = "4g"

[tx-store]
path = "/data/tx-store"
default-snomed-edition = "32506021000036107"
expansion-cache-size = 200
default-dialect = "en-AU"

[tx-store.dialect-aliases]
en-NZ = "271000210107"
```

## Validation[​](#validation "Direct link to Validation")

Unknown keys, at the top level or within `[terminology-auth]`, `[bulk-auth]` or `[tx-store]`, produce a warning that names the key and lists the valid keys; the key is then ignored. A value of the wrong type for a `[tx-store]` key, or an invalid `[spark]` key or value, is a usage error (exit code 2) reported before any Spark session starts.

## Secret values[​](#secret-values "Direct link to Secret values")

Values marked as secrets below accept either a literal or a `@/path/to/file` reference, whose contents (with surrounding whitespace stripped) are used as the value. This keeps secrets out of both the config file and shell history. An unreadable file is an error.

## Top-level keys[​](#top-level-keys "Direct link to Top-level keys")

| Key                  | Type   | Flag                                             | Default                               | Description                                                                                                                                                               |
| -------------------- | ------ | ------------------------------------------------ | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tx-server`          | string | `--tx-server`                                    | `https://tx.ontoserver.csiro.au/fhir` | The FHIR terminology server used by the terminology functions. Ignored, with a warning, when a [local terminology store](#tx-store) is configured.                        |
| `fhir-version`       | string | `--fhir-version`                                 | `R4`                                  | The FHIR version of the data. `R4` is the only supported value.                                                                                                           |
| `package-registry`   | string | `--package-registry` (`import-fhir-terminology`) | `https://packages.fhir.org`           | The FHIR package registry that `import-fhir-terminology` checks a package against. See [terminology import commands](/docs/libraries/cli.md#terminology-import-commands). |
| `[terminology-auth]` | table  | -                                                | none                                  | [Terminology server authentication](#terminology-auth).                                                                                                                   |
| `[bulk-auth]`        | table  | -                                                | none                                  | [Bulk export authentication](#bulk-auth).                                                                                                                                 |
| `[tx-store]`         | table  | -                                                | none                                  | [Local terminology store](#tx-store).                                                                                                                                     |
| `[spark]`            | table  | `--spark-conf KEY=VALUE`                         | none                                  | [Spark configuration](#spark-configuration).                                                                                                                              |

`package-registry` is a top-level key rather than part of `[tx-store]`, because it concerns where a package was distributed from rather than the store the package is imported into.

## `[terminology-auth]`[​](#terminology-auth "Direct link to terminology-auth")

OAuth2 client credentials for the terminology server. See [terminology server authentication](/docs/libraries/terminology/server.md#authentication).

| Key              | Type   | Flag                  | Description                   |
| ---------------- | ------ | --------------------- | ----------------------------- |
| `client-id`      | string | `--tx-client-id`      | The OAuth2 client identifier. |
| `client-secret`  | secret | `--tx-client-secret`  | The OAuth2 client secret.     |
| `token-endpoint` | string | `--tx-token-endpoint` | The OAuth2 token endpoint.    |
| `scope`          | string | `--tx-scope`          | The OAuth2 scope to request.  |

Authentication is enabled only when both a client identifier and a token endpoint are present. If some values are supplied but either of these is missing, a warning is printed and authentication is disabled. The table is ignored, with a warning, when a [local terminology store](#tx-store) is configured. `private-key-jwk` is accepted without a warning, because the two authentication tables share a key set, but it has no effect here.

## `[bulk-auth]`[​](#bulk-auth "Direct link to bulk-auth")

SMART backend services credentials for the [`export`](/docs/libraries/cli.md#export) command.

| Key               | Type   | Flag                | Environment variable       | Description                       |
| ----------------- | ------ | ------------------- | -------------------------- | --------------------------------- |
| `client-id`       | string | `--client-id`       | -                          | The client identifier.            |
| `token-endpoint`  | string | `--token-endpoint`  | -                          | The token endpoint.               |
| `client-secret`   | secret | `--client-secret`   | `PATHLING_CLIENT_SECRET`   | A client secret.                  |
| `private-key-jwk` | secret | `--private-key-jwk` | `PATHLING_PRIVATE_KEY_JWK` | A private key, as a JSON Web Key. |
| `scope`           | string | `--scope`           | -                          | The scope to request.             |

Each value is resolved with the precedence flag > config file > environment variable. With no authentication input at all, the export runs unauthenticated. Otherwise `client-id` and `token-endpoint` are required, along with exactly one of `client-secret` or `private-key-jwk`; any other combination is a usage error.

## `[tx-store]`[​](#tx-store "Direct link to tx-store")

A local terminology store, which selects [local terminology mode](/docs/libraries/cli.md#local-terminology-mode).

| Key                      | Type             | Flag                                  | Default                 | Description                                                                                                                                         |
| ------------------------ | ---------------- | ------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`                   | string           | `--tx-store`                          | none                    | The store location, as a filesystem path or a URI. Its presence selects local mode. Also the default `STORAGE_PATH` for the import commands.        |
| `default-snomed-edition` | string           | -                                     | the library default     | The SNOMED CT module identifier used to disambiguate an unversioned SNOMED CT reference when the store holds multiple editions.                     |
| `expansion-cache-size`   | positive integer | -                                     | the library default     | The maximum number of value set expansions cached per executor.                                                                                     |
| `default-dialect`        | string           | `--default-dialect` (`import-snomed`) | chosen from the release | The dialect whose preferred synonyms become each concept's stored display on import: a tag such as `en-GB`, or a language reference set identifier. |
| `[dialect-aliases]`      | table of strings | -                                     | none                    | Additional dialect tags. See below.                                                                                                                 |

The `[tx-store.dialect-aliases]` table registers additional dialect tags, mapping a language tag to the identifier of the SNOMED CT language reference set that serves it. An entry for a tag that is already recognised replaces the built-in mapping for it. A value that is not a table of strings is reported and ignored rather than being fatal, since an unrecognised tag simply expresses no preference. See [dialects](/docs/libraries/terminology/local.md#dialects).

A `[tx-store]` table without a `path` (and without `--tx-store`) has no effect, and a warning says that remote mode will be used. A scalar `tx-store = "/path"` is ignored with a warning; use a `[tx-store]` table with a `path` key instead.

## Spark configuration[​](#spark-configuration "Direct link to Spark configuration")

The `[spark]` table sets arbitrary [Apache Spark](https://spark.apache.org/) properties on the session that every data command builds. Use it to tune the engine - for example to raise executor memory, change the shuffle partition count, or add a cloud storage connector - or pass one or more `--spark-conf KEY=VALUE` flags for a single invocation. The flag is repeatable and overrides the `[spark]` value for the same key; when the same key is given more than once on the command line, the last occurrence wins. Because the value is split on the first `=` only, a value may itself contain `=` (for example `--spark-conf spark.driver.extraJavaOptions=-Dfoo=bar`).

```
[spark]
"spark.sql.shuffle.partitions" = 16
"spark.sql.adaptive.enabled" = true
"spark.executor.memory" = "4g"
"spark.jars.packages" = "org.apache.hadoop:hadoop-aws:3.4.1"
"spark.hadoop.fs.s3a.secret.key" = "@/run/secrets/s3-key"
```

The resolved settings are merged with Pathling's own required Spark defaults rather than replacing them, so Pathling keeps working while your tuning takes effect. The full precedence, highest first, is:

1. A `--spark-conf` flag value.
2. The `[spark]` table value in the chosen config file.
3. The CLI's quiet-mode logging settings (applied only when not `--verbose`, and only for keys you did not set).
4. Pathling's managed defaults (always present for the managed keys below).

**Keys and values.** Every key must begin with `spark.`; any other key is an error that names the offending key and aborts before a Spark session starts. Values may be strings, integers, floats, or booleans, and are coerced to the strings Spark expects (booleans become `true`/`false`). TOML arrays and tables are not accepted; list-valued properties use Spark's native comma-separated string. A string value may be a `@/path/to/file` [secret](#secret-values) reference.

**Managed keys.** Three keys are owned by Pathling and merged item by item so the library is never broken:

* `spark.jars.packages` - your coordinates are unioned with Pathling's managed coordinates (the library runtime and Delta Lake) and deduplicated. Supplying a managed coordinate at a different version is allowed and applies your version, but prints a warning naming the coordinate, since a non-default version may not be supported.
* `spark.sql.extensions` - your extension class names are unioned with Pathling's, and the Delta extension always remains present.
* `spark.sql.catalog.spark_catalog` - this is locked to Pathling's Delta catalog. Setting it to that value is a no-op; setting it to anything else is an error.

**Quiet-mode logging.** By default the CLI suppresses Spark and JVM logging by setting `spark.driver.extraJavaOptions`. If you set that key yourself, your value replaces the CLI's, so Spark logging is no longer suppressed. To keep both, run with `--verbose`, or include the quiet log4j2 option in the value you supply.
