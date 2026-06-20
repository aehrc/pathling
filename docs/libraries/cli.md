# Command line interface

The `pathling` command line interface surfaces the functionality of the Pathling Python library through concise, scriptable commands. It is shipped as a console script within the `pathling` Python package, so its version always matches the library version.

Each invocation starts a fresh Spark session. The cold start (typically 10-30 seconds) is communicated through a progress indicator on standard error. Data is written to standard output, while progress, status, and errors are written to standard error, so that piped output stays clean.

## Installation[​](#installation "Direct link to Installation")

The CLI is part of the `pathling` package and requires a supported Java runtime to be present (as already required by the underlying PySpark dependency). The CLI reports the absence of Java clearly but does not install it.

The simplest way to run it is with [uv](https://docs.astral.sh/uv/):

```
# Run without installing.
uvx pathling --version

# Or install the tool so that `pathling` is on your PATH.
uv tool install pathling
pathling --help
```

## Global options[​](#global-options "Direct link to Global options")

The following options are accepted before any command and may also be supplied through a configuration file.

| Option                                                                      | Config key           | Default                                 |
| --------------------------------------------------------------------------- | -------------------- | --------------------------------------- |
| `--tx-server`                                                               | `tx-server`          | the library default terminology server  |
| `--tx-client-id`, `--tx-client-secret`, `--tx-token-endpoint`, `--tx-scope` | `[terminology-auth]` | none                                    |
| `--fhir-version`                                                            | `fhir-version`       | `R4`                                    |
| `--spark-conf KEY=VALUE`                                                    | `[spark]`            | none                                    |
| `--config PATH`                                                             | -                    | `$XDG_CONFIG_HOME/pathling/config.toml` |
| `--verbose`                                                                 | -                    | off                                     |

Values are resolved with the precedence flag > config file > built-in default. The `--verbose` flag re-enables Spark and JVM logging and prints full stack traces on error.

Exit codes are `0` for success, `1` for a runtime failure, and `2` for a usage error.

## Data source inputs[​](#data-source-inputs "Direct link to Data source inputs")

Commands that read FHIR data take a positional `SOURCE` path. The format is auto-detected from the path contents (ndjson, FHIR Bundles, Parquet, or Delta) and can be set explicitly with `--from ndjson|bundles|parquet|delta`. If the format cannot be determined, the error lists what was found and shows how to specify `--from`.

## Output options[​](#output-options "Direct link to Output options")

Tabular results (from `view`, `fhirpath`, and the terminology commands) render as a human-readable table by default.

| Option                           | Behaviour                                                                                                              |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `--format`                       | `table` (default), `csv`, `ndjson`; with `-o` also `parquet`, `delta`.                                                 |
| `-o PATH`                        | Write to a file instead of stdout; the format is inferred from the extension (`.csv`, `.ndjson`/`.jsonl`, `.parquet`). |
| `--limit N`                      | Row cap for stdout table output (default 50).                                                                          |
| `--overwrite`                    | Allow replacing an existing output path.                                                                               |
| `--departition/--no-departition` | Write file output as a single file (default) or as a Spark directory of part files. No effect on Delta.                |

File output is produced by Spark's distributed writers, so results larger than driver memory can be written. By default the output is departitioned to a single file at the path given; pass `--no-departition` to keep Spark's native directory of part files. Delta output is always written as a table directory.

For scripted use, prefer `--format csv` or `--format ndjson`, which stream the full result.

## Commands[​](#commands "Direct link to Commands")

### convert[​](#convert "Direct link to convert")

Convert FHIR data between formats.

```
pathling convert data/ --to parquet -o warehouse/
pathling convert bundles/ --from bundles --to ndjson -o out/
pathling convert bundles/ --from bundles --type Patient --type Condition --to ndjson -o out/
```

The `--mode overwrite|error|append|merge` option controls the save mode for Parquet and Delta output (`merge` is valid for Delta only). For a Bundles source, the repeatable `--type` option names the resource types to read; when given, those types are used directly and the driver-side discovery pass is skipped. A summary of the resource types written and the output location is printed at the end.

### view[​](#view "Direct link to view")

Run a SQL on FHIR ViewDefinition against a data source.

```
pathling view data/ --view patients.json
pathling view data/ --view patients.json --format csv
pathling view data/ --view-json '{"resource":"Patient", ...}' -o results.parquet
```

The view is supplied as a file (`--view`) or inline JSON (`--view-json`). A `--filter` FHIR search expression restricts the resources processed.

### fhirpath[​](#fhirpath "Direct link to fhirpath")

Evaluate a FHIRPath expression.

```
# Data source mode: one row per resource with its id and result.
pathling fhirpath data/ -t Patient -e 'name.family'

# Single resource mode: the typed result values.
pathling fhirpath patient.json -e 'name.given.first()'
```

In data source mode, `-t/--type` selects the subject resource type and `--filter` restricts the resources processed. In single resource mode (when `SOURCE` is a single FHIR resource JSON file), `--context` and repeatable `--var name=value` options are supported.

Both modes name the result column `result`: data source mode pairs it with each resource's `id` (one row per resource), while single resource mode pairs it with each result item's `type` (one row per result item).

### export[​](#export "Direct link to export")

Bulk export data from a FHIR server.

```
pathling export https://server/fhir -o out/ --type Patient
pathling export https://server/fhir -o out/ --group 123
```

A system-level export runs by default; `--group ID` and repeatable `--patient REF` select group-level and patient-level exports (the two are mutually exclusive). The export supports `--type`, `--elements`, `--since`, `--type-filter`, `--include-associated-data`, `--timeout`, and `--max-downloads`.

SMART backend services authentication is configured with `--client-id`, `--token-endpoint`, `--scope`, and exactly one of `--private-key-jwk` or `--client-secret`. Secret values accept a literal, a `@/path/to/file` reference, or fall back to the `PATHLING_PRIVATE_KEY_JWK` / `PATHLING_CLIENT_SECRET` environment variables so that they need not appear in shell history.

### run[​](#run "Direct link to run")

Execute Python code with the Pathling environment ready. The code runs with two variables already in scope: `spark` (the Spark session) and `pathling` (the configured Pathling context), built with the same configuration resolution as every other command.

```
# Run a script file.
pathling run my_script.py

# Run an inline one-liner.
pathling run -c "print(spark.version)"

# Pipe a script through stdin.
cat job.py | pathling run -

# Pass arguments through to the script.
pathling run etl.py --input data.ndjson out/
```

For example, a script that projects a tabular view of patients and then summarises it with Spark SQL:

```
patients = pathling.read.ndjson("data").view(
    "Patient",
    select=[{"column": [{"path": "gender", "name": "gender"}]}],
)
patients.createOrReplaceTempView("patient")
spark.sql("SELECT gender, count(*) AS count FROM patient GROUP BY gender").show()
```

The code source is exactly one of a script path, `-` (standard input), or `-c CODE`; supplying both a script and `-c`, or neither, is a usage error (exit code 2), reported before the Spark session is started.

Execution follows Python interpreter semantics: the module runs as `__main__`; for file scripts `__file__` is set and the script's directory is prepended to `sys.path`; trailing arguments arrive in `sys.argv` with `sys.argv[0]` being the script path, `-c`, or `-` as the interpreter would set it. Dash-prefixed arguments pass through to the script rather than being parsed by the CLI.

The exit code is `0` on success and `1` for an uncaught exception or syntax error, with a standard Python traceback (no CLI frames) printed to standard error. `sys.exit(n)` exits with `n` and no traceback. The script's stdout passes through unmodified.

### console[​](#console "Direct link to console")

Open an interactive [IPython](https://ipython.org/) console with the same `spark` and `pathling` variables in scope.

```
pathling console
```

After the startup progress indicator, a banner identifies the Pathling version and the variables in scope. Errors evaluated at the prompt show normal tracebacks without ending the session; leave with `exit` or Ctrl-D (exit code 0).

### Terminology commands[​](#terminology-commands "Direct link to Terminology commands")

The `member-of`, `translate`, `subsumes`, `subsumed-by`, `display`, `property-of`, and `designation` commands read a tabular dataset (CSV or Parquet), build codings from a `--code-column` plus either a fixed `--system` URI or a `--system-column`, and append the operation's result column(s).

```
pathling member-of codes.csv --code-column code \
  --system http://snomed.info/sct \
  --value-set 'http://snomed.info/sct?fhir_vs=refset/...'

pathling translate codes.csv --code-column code \
  --system http://snomed.info/sct --concept-map '<uri>'
```

The default result column names (`member_of`, `translated_system` and `translated_code`, `subsumes`, `subsumed_by`, `display`, `property`, `designation`) can be overridden with `--result-column`.

#### Subsumption against a fixed target coding[​](#subsumption-against-a-fixed-target-coding "Direct link to Subsumption against a fixed target coding")

The `subsumes` and `subsumed-by` commands test each row's coding against a target coding. The target code can come from a second column (`--other-code-column`) or be a single fixed value applied to every row (`--other-code`); exactly one of the two must be given. This lets you test a column of codes against one known concept without first adding a constant column to the data. The target system is supplied with `--other-system` (a fixed URI) or `--other-system-column` (a per-row column), and `--system-version` applies to both codings.

```
# Fixed target coding: is each code subsumed by Diabetes mellitus?
pathling subsumed-by codes.csv --code-column code \
  --system http://snomed.info/sct \
  --other-code 73211009 --other-system http://snomed.info/sct

# Two-column comparison: does each code in column a subsume the code in column b?
pathling subsumes pairs.csv --code-column a \
  --system http://snomed.info/sct \
  --other-code-column b --other-system http://snomed.info/sct
```

Invalid combinations - supplying both or neither of `--other-code` / `--other-code-column`, or both or neither of `--other-system` / `--other-system-column` - are reported as usage errors before any Spark session starts.

## Configuration file[​](#configuration-file "Direct link to Configuration file")

Defaults for the global options can be set in a TOML file at `${XDG_CONFIG_HOME:-~/.config}/pathling/config.toml`:

```
tx-server = "https://tx.example.org/fhir"
fhir-version = "R4"

[terminology-auth]
client-id = "my-client"
client-secret = "..."
token-endpoint = "https://auth.example.org/token"

[bulk-auth]
client-id = "bulk-client"
token-endpoint = "https://auth.example.org/token"

[spark]
"spark.sql.shuffle.partitions" = 16
"spark.executor.memory" = "4g"
```

Command-line flags always take precedence over the config file. Unknown keys produce a warning that names the key and lists the valid keys.

### Spark configuration[​](#spark-configuration "Direct link to Spark configuration")

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

**Keys and values.** Every key must begin with `spark.`; any other key is an error that names the offending key and aborts before a Spark session starts. Values may be strings, integers, floats, or booleans, and are coerced to the strings Spark expects (booleans become `true`/`false`). TOML arrays and tables are not accepted; list-valued properties use Spark's native comma-separated string. A string value may be a `@/path/to/file` secret reference, read exactly as authentication secrets are.

**Managed keys.** Three keys are owned by Pathling and merged item by item so the library is never broken:

* `spark.jars.packages` - your coordinates are unioned with Pathling's managed coordinates (the library runtime and Delta Lake) and deduplicated. Supplying a managed coordinate at a different version is allowed and applies your version, but prints a warning naming the coordinate, since a non-default version may not be supported.
* `spark.sql.extensions` - your extension class names are unioned with Pathling's, and the Delta extension always remains present.
* `spark.sql.catalog.spark_catalog` - this is locked to Pathling's Delta catalog. Setting it to that value is a no-op; setting it to anything else is an error.

**Quiet-mode logging.** By default the CLI suppresses Spark and JVM logging by setting `spark.driver.extraJavaOptions`. If you set that key yourself, your value replaces the CLI's, so Spark logging is no longer suppressed. To keep both, run with `--verbose`, or include the quiet log4j2 option in the value you supply.

### Project-local configuration[​](#project-local-configuration "Direct link to Project-local configuration")

The CLI also looks for a file named `pathling.toml` in the current working directory. When present, it is used in place of the user-level config file - not merged with it. This lets a project carry its own settings (for example a particular terminology server) without editing your personal config or passing `--config` on every command.

Exactly one config file is ever read, chosen by this precedence (highest first):

1. The path given to `--config`.
2. A `pathling.toml` in the current working directory.
3. The user-level `config.toml`.
4. None, in which case built-in defaults apply.

Because files are never merged, any key the chosen file omits falls back to its built-in default rather than to a value from another file. Discovery is limited to the current working directory; the CLI does not search parent directories.

When a `pathling.toml` is discovered and used, the CLI prints a one-line notice on standard error naming the file (and the user-level file it overrides, when one exists), so the active configuration is never a surprise:

```
Using project config /path/to/pathling.toml (overrides ~/.config/pathling/config.toml).
```

Passing an explicit `--config` skips project-local discovery, and no such notice is printed. An explicit `--config` path must exist; a missing path is an error rather than a silent fall-back to another config file.
