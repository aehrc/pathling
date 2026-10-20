---
sidebar_position: 7
description: The Pathling command line interface surfaces the Python library's functionality - data conversion, SQL on FHIR views, FHIRPath evaluation, bulk export, terminology operations, Python scripting, and an interactive console - through scriptable commands.
---

# Command line interface

The `pathling` command line interface surfaces the functionality of the
Pathling Python library through concise, scriptable commands. It is shipped as
a console script within the `pathling` Python package, so its version always
matches the library version.

Each invocation starts a fresh Spark session. The cold start (typically 10-30
seconds) is communicated through a progress indicator on standard error. Data
is written to standard output, while progress, status, and errors are written
to standard error, so that piped output stays clean.

## Installation

The CLI is part of the `pathling` package and requires a supported Java runtime
to be present (as already required by the underlying PySpark dependency). The
CLI reports the absence of Java clearly but does not install it.

The simplest way to run it is with [uv](https://docs.astral.sh/uv/):

```bash
# Run without installing.
uvx pathling --version

# Or install the tool so that `pathling` is on your PATH.
uv tool install pathling
pathling --help
```

## Global options

The following options are accepted before any command and may also be supplied
through the [configuration file](./configuration.md).

| Option                                                                      | Config key                   | Default                                 |
| --------------------------------------------------------------------------- | ---------------------------- | --------------------------------------- |
| `--tx-server`                                                               | `tx-server`                  | the library default terminology server  |
| `--tx-store`                                                                | `tx-store.path`              | none (remote mode)                      |
| -                                                                           | `[tx-store.dialect-aliases]` | none                                    |
| `--tx-client-id`, `--tx-client-secret`, `--tx-token-endpoint`, `--tx-scope` | `[terminology-auth]`         | none                                    |
| `--fhir-version`                                                            | `fhir-version`               | `R4`                                    |
| `--spark-conf KEY=VALUE`                                                    | `[spark]`                    | none                                    |
| `--config PATH`                                                             | -                            | `$XDG_CONFIG_HOME/pathling/config.toml` |
| `--verbose`                                                                 | -                            | off                                     |

Values are resolved with the precedence flag > config file > built-in default;
see [configuration](./configuration.md) for every config key.
The `--verbose` flag re-enables Spark and JVM logging and prints full stack
traces on error.

Exit codes are `0` for success, `1` for a runtime failure, and `2` for a usage
error.

## Data source inputs

Commands that read FHIR data take a positional `SOURCE` path. The format is
auto-detected from the path contents (ndjson, FHIR Bundles, Parquet, or Delta)
and can be set explicitly with `--from ndjson|bundles|parquet|delta`. If the
format cannot be determined, the error lists what was found and shows how to
specify `--from`.

## Output options

Tabular results (from `view`, `fhirpath`, and the terminology commands) render
as a human-readable table by default.

| Option                           | Behaviour                                                                                                                                                    |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `--format`                       | `table` (default), `csv`, `ndjson`; with `-o` also `parquet`, `delta`.                                                                                       |
| `-o PATH`                        | Write to a file instead of stdout; the format, and for `.tsv` the delimiter, is inferred from the extension (`.csv`/`.tsv`, `.ndjson`/`.jsonl`, `.parquet`). |
| `--limit N`                      | Row cap for stdout table output (default 50).                                                                                                                |
| `--overwrite`                    | Allow replacing an existing output path.                                                                                                                     |
| `--departition/--no-departition` | Write file output as a single file (default) or as a Spark directory of part files. No effect on Delta.                                                      |
| `--delimiter CHAR`               | Field separator for CSV input and output (default: a tab for a `.tsv` path, otherwise `,`). Accepts an escape such as `\t`.                                  |
| `--header/--no-header`           | Include a header row in CSV output (default enabled).                                                                                                        |

File output is produced by Spark's distributed writers, so results larger than
driver memory can be written. By default the output is departitioned to a
single file at the path given; pass `--no-departition` to keep Spark's native
directory of part files. Delta output is always written as a table directory.

For scripted use, prefer `--format csv` or `--format ndjson`, which stream the
full result.

The `--delimiter` and `--header/--no-header` options apply to CSV output for
`view`, `fhirpath`, and the terminology commands, and are ignored for non-CSV
formats. The delimiter is also used to read CSV input in the terminology
commands.

`--delimiter` has no fixed default. When it is omitted, the input side and the
output side each take their default from their own path: a `.tsv` extension (in
any letter case) means a tab, and anything else means a comma. CSV written to
standard output, where there is no path, is comma-separated. A tab-separated
dataset therefore round-trips with no delimiter flag at all:

```bash
pathling member-of codes.tsv --code-column code \
  --system http://snomed.info/sct --value-set <uri> \
  -o out.tsv
```

Because the two sides resolve independently, `codes.tsv -o out.csv` reads with a
tab and writes with a comma. A delimiter given explicitly applies to both sides
and overrides both extensions, so `--delimiter ','` on a `.tsv` path yields a
comma. Whenever a tab is derived from an extension rather than typed, the CLI
says so on standard error, naming the path.

## Commands

### convert

Convert FHIR data between formats.

```bash
pathling convert data/ --to parquet -o warehouse/
pathling convert bundles/ --from bundles --to ndjson -o out/
pathling convert bundles/ --from bundles --type Patient --type Condition --to ndjson -o out/
```

The `--mode overwrite|error|append|merge` option controls the save mode for
Parquet and Delta output (`merge` is valid for Delta only). For a Bundles
source, the repeatable `--type` option names the resource types to read; when
given, those types are used directly and the driver-side discovery pass is
skipped. A summary of the resource types written and the output location is
printed at the end.

### view

Run a SQL on FHIR ViewDefinition against a data source.

```bash
pathling view data/ --view patients.json
pathling view data/ --view patients.json --format csv
pathling view data/ --view-json '{"resource":"Patient", ...}' -o results.parquet
```

The view is supplied as a file (`--view`) or inline JSON (`--view-json`). A
`--filter` FHIR search expression restricts the resources processed.

### fhirpath

Evaluate a FHIRPath expression.

```bash
# Data source mode: one row per resource with its id and result.
pathling fhirpath data/ -t Patient -e 'name.family'

# Single resource mode: the typed result values.
pathling fhirpath patient.json -e 'name.given.first()'
```

In data source mode, `-t/--type` selects the subject resource type and
`--filter` restricts the resources processed. In single resource mode (when
`SOURCE` is a single FHIR resource JSON file), `--context` and repeatable
`--var name=value` options are supported.

Both modes name the result column `result`: data source mode pairs it with each
resource's `id` (one row per resource), while single resource mode pairs it with
each result item's `type` (one row per result item).

### export

Bulk export data from a FHIR server.

```bash
pathling export https://server/fhir -o out/ --type Patient
pathling export https://server/fhir -o out/ --group 123
```

A system-level export runs by default; `--group ID` and repeatable
`--patient REF` select group-level and patient-level exports (the two are
mutually exclusive). The export supports `--type`, `--elements`, `--since`,
`--type-filter`, `--include-associated-data`, `--timeout`, and
`--max-downloads`.

SMART backend services authentication is configured with `--client-id`,
`--token-endpoint`, `--scope`, and exactly one of `--private-key-jwk` or
`--client-secret`. Secret values accept a literal, a `@/path/to/file` reference,
or fall back to the `PATHLING_PRIVATE_KEY_JWK` / `PATHLING_CLIENT_SECRET`
environment variables so that they need not appear in shell history. The same
settings can be recorded in the [`[bulk-auth]`](./configuration.md#bulk-auth)
config table.

### run

Execute Python code with the Pathling environment ready. The code runs with
two variables already in scope: `spark` (the Spark session) and `pc`
(the configured Pathling context), built with the same configuration
resolution as every other command.

The Pathling public functions are also pre-imported, so no
`from pathling import ...` line is needed. This covers the terminology and
coding functions (`to_coding`, `to_snomed_coding`, `to_loinc_coding`,
`member_of`, `translate`, `subsumes`, and so on), the argument helper types
(`Coding`, `PropertyType`, `Equivalence`), and the API types (`PathlingContext`,
`DataSource`, and the rest). The pre-imported names are exactly the
[public functions and types listed in the Python API
reference](https://pathling.csiro.au/docs/python/pathling.html#module-pathling).
The terminology display function is bound under both its natural name `display`
and the alias `tx_display`. The name `pathling` is bound to the package module
itself, so a bare `import pathling` is a harmless no-op that leaves `pc` intact.
Any pre-imported name is only a default: assigning or defining the same name in
your own code takes precedence, and an explicit `from pathling import ...`
continues to work unchanged.

```bash
# Run a script file.
pathling run my_script.py

# Run an inline one-liner.
pathling run -c "print(spark.version)"

# Pipe a script through stdin.
cat job.py | pathling run -

# Pass arguments through to the script.
pathling run etl.py --input data.ndjson out/
```

For example, a script that projects a tabular view of patients and then
summarises it with Spark SQL:

```python
patients = pc.read.ndjson("data").view(
    "Patient",
    select=[{"column": [{"path": "gender", "name": "gender"}]}],
)
patients.createOrReplaceTempView("patient")
spark.sql("SELECT gender, count(*) AS count FROM patient GROUP BY gender").show()
```

The code source is exactly one of a script path, `-` (standard input), or
`-c CODE`; supplying both a script and `-c`, or neither, is a usage error
(exit code 2), reported before the Spark session is started.

Execution follows Python interpreter semantics: the module runs as
`__main__`; for file scripts `__file__` is set and the script's directory is
prepended to `sys.path`; trailing arguments arrive in `sys.argv` with
`sys.argv[0]` being the script path, `-c`, or `-` as the interpreter would
set it. Dash-prefixed arguments pass through to the script rather than being
parsed by the CLI.

The exit code is `0` on success and `1` for an uncaught exception or syntax
error, with a standard Python traceback (no CLI frames) printed to standard
error. `sys.exit(n)` exits with `n` and no traceback. The script's stdout
passes through unmodified.

### console

Open an interactive [IPython](https://ipython.org/) console with the same
`spark` and `pc` variables in scope, and the Pathling public functions
pre-imported just as in [`run`](#run).

```bash
pathling console
```

After the startup progress indicator, a banner identifies the Pathling
version, the variables in scope, and the pre-imported functions. Errors
evaluated at the prompt show normal tracebacks without ending the session;
leave with `exit` or Ctrl-D (exit code 0).

The one difference from `run` is the terminology display function. IPython
installs its own `display` at the prompt, so Pathling's terminology display is
bound as `tx_display` only; `display` remains IPython's built-in. A snippet that
uses `tx_display` therefore behaves the same in the console and in a script.

### Terminology commands

The `member-of`, `translate`, `subsumes`, `subsumed-by`, `display`,
`property-of`, and `designation` commands read a tabular dataset (CSV, Parquet,
or Delta), build codings from a `--code-column` plus either a fixed `--system`
URI or a `--system-column`, and append the operation's result column(s).

```bash
pathling member-of codes.csv --code-column code \
  --system http://snomed.info/sct \
  --value-set 'http://snomed.info/sct?fhir_vs=refset/...'

pathling translate codes.csv --code-column code \
  --system http://snomed.info/sct --concept-map '<uri>'
```

In [local terminology mode](#local-terminology-mode), the forms accepted by
`--value-set` and `--concept-map` - imported canonical URLs, the SNOMED CT
implicit forms, ECL, and VCL - are listed under
[value set and concept map expressions](../terminology/local.md#value-set-and-concept-map-expressions).
Against a terminology server, they are whatever that server supports.

The input format is set with `--from csv|parquet|delta`. When omitted, it is
auto-detected from the dataset path: files ending in `.csv` or `.parquet` are
read as CSV or Parquet; a directory containing a `_delta_log` entry is read as
Delta; and any other directory containing at least one `.parquet` file is read
as Parquet. CSV inputs are read with a header row by default and all columns as
strings. Passing `--from` bypasses detection, which is useful for Delta tables or CSV
files with an unconventional extension. An input whose format cannot be
determined - an unrecognised suffix, or a directory with neither a `_delta_log`
entry nor `.parquet` files - is reported as a usage error before any Spark
session starts. This also lets you read the CLI's own Parquet or Delta output
directory straight back into another terminology command.

```bash
# Explicit Delta input.
pathling display warehouse/codes --from delta \
  --code-column code --system http://snomed.info/sct

# Auto-detected Delta directory (contains _delta_log).
pathling member-of warehouse/codes --code-column code \
  --system http://snomed.info/sct \
  --value-set 'http://snomed.info/sct?fhir_vs=refset/...'
```

The default result column names (`member_of`, `translated_system` and
`translated_code`, `subsumes`, `subsumed_by`, `display`, `property`,
`designation`) can be overridden with `--result-column`.

CSV input is read with the shared `--delimiter` (so a semicolon-separated
dataset is read correctly); a `.tsv` file is read as CSV and, with no
`--delimiter`, as tab-separated. Pass `--no-input-header`
to read a headerless CSV; its columns are then addressable by the positional names
Spark assigns (`_c0`, `_c1`, ...), which you reference via `--code-column`,
`--system-column`, and the other column options.

```bash
pathling member-of headerless.csv --no-input-header --code-column _c0 \
  --system http://snomed.info/sct --value-set '<uri>'
```

#### Subsumption against a fixed target coding

The `subsumes` and `subsumed-by` commands test each row's coding against a
target coding. The target code can come from a second column
(`--other-code-column`) or be a single fixed value applied to every row
(`--other-code`); exactly one of the two must be given. This lets you test a
column of codes against one known concept without first adding a constant
column to the data. The target system is supplied with `--other-system` (a
fixed URI) or `--other-system-column` (a per-row column), and `--system-version`
applies to both codings.

```bash
# Fixed target coding: is each code subsumed by Diabetes mellitus?
pathling subsumed-by codes.csv --code-column code \
  --system http://snomed.info/sct \
  --other-code 73211009 --other-system http://snomed.info/sct

# Two-column comparison: does each code in column a subsume the code in column b?
pathling subsumes pairs.csv --code-column a \
  --system http://snomed.info/sct \
  --other-code-column b --other-system http://snomed.info/sct
```

Invalid combinations - supplying both or neither of `--other-code` /
`--other-code-column`, or both or neither of `--other-system` /
`--other-system-column` - are reported as usage errors before any Spark session
starts.

### Terminology import commands

The `import-snomed` and `import-fhir-terminology` commands import terminology
content into a local terminology store for use with
[local terminology mode](../terminology/local.md). Both take a
`SOURCE` path and a `STORAGE_PATH` for the store, report progress, and print a
completion summary.

```bash
pathling import-snomed /data/rf2.zip /data/tx-store
pathling import-fhir-terminology /data/hl7.terminology.tgz /data/tx-store
```

`import-snomed` accepts `--edition-uri` to override the detected SNOMED
edition/version, and `--dense-id-order pre-order` to assign internal concept
identifiers by a depth-first traversal of the is-a hierarchy, which makes the
hierarchy index materially smaller at query time in exchange for identifiers that
shift more between releases - see
[reducing the memory the hierarchy takes at query time](../terminology/local.md#reducing-the-memory-the-hierarchy-takes-at-query-time).

It also accepts `--default-dialect`, which names the dialect whose preferred
synonyms become each concept's stored display: a tag such as `en-GB`, or a
language reference set identifier. When the flag is omitted, the
`tx-store.default-dialect` config key applies; when neither is set, the dialect
is chosen from the release. A release holding several language reference sets
that is not the International edition fails the import, listing the candidates,
so that one can be named - see [dialects](../terminology/local.md#dialects).

```bash
pathling import-snomed --default-dialect en-GB /data/rf2.zip /data/tx-store
```

`import-fhir-terminology` accepts a JSON file, a directory of JSON files, or a
FHIR NPM package (`.tgz`), and imports CodeSystems of any size with bounded
memory (for example, the multi-gigabyte OMOP vocabulary CodeSystem).

A package source is checked against the checksum its registry publishes before
anything is written: the import reads the package name and version from the
package's `package.json`, fetches the registry's version listing, and compares
the SHA-1 the registry publishes for that version with the SHA-1 of the tarball
in hand. A package whose checksum differs fails the import with exit code 1,
leaves the store unchanged, and reports the expected and computed SHA-1
followed by `Re-run with --no-verify to import it anyway.` Where no comparison
can be made, because the registry publishes no checksum for the version, does
not list it, or cannot be reached, the import warns and proceeds. See
[provenance and verification](../terminology/local.md#provenance-and-verification)
for what the store records.

Two options control the check. Both apply only when `SOURCE` is a `.tgz` or
`.tar.gz`; for any other source they are accepted and have no effect.

- `--no-verify` imports without consulting a registry. Use it for an offline
  import, or a package that was never published to a registry.
- `--package-registry URL` names the registry to check against. The flag
  overrides the `package-registry` config key (see
  [configuration](./configuration.md#top-level-keys)), which overrides the
  default of `https://packages.fhir.org`.

`import-snomed` takes neither option, since an RF2 release is not distributed
through a package registry.

Each command prints one completion line naming what was imported, from where
and into where, such as `Imported FHIR terminology from SRC into STORE`,
followed by what the store recorded of the source:

| Case                          | Suffix on the completion line                                                                      |
| ----------------------------- | -------------------------------------------------------------------------------------------------- |
| Directory source              | none                                                                                               |
| RF2 `.zip` or JSON file       | `(sha256 HEX)`                                                                                     |
| Package, verified             | `(verified NAME VERSION against REGISTRY; sha256 HEX)`                                             |
| Package, not verified         | `(NAME VERSION not verified against a registry; re-run with --verbose for the reason; sha256 HEX)` |
| Package, verification skipped | `(NAME VERSION, verification skipped; sha256 HEX)`                                                 |

A directory has no single set of bytes to fingerprint, so it carries no hash
and no verification statement. Where the package declares no name or version,
`NAME VERSION` is replaced by the word `package`, giving
`(package not verified against a registry; …)` and
`(package verification skipped; …)`. The reason a package was not verified is
in the library's warning log, which `--verbose` shows, as for every other
import message.

An RF2 source given to `import-snomed` must be
[self-contained](../terminology/local.md#rf2-sources-must-be-self-contained): rows
referencing concepts the source does not itself ship are dropped, which is the
ordinary shape of a derived or extension package supplied without the release it
depends on. The import reports, for each file, how many of its active rows
resolved, which is how a shortfall is detected. The same section gives a
[recipe for combining a package with its dependency](../terminology/local.md#combining-a-derived-package-with-its-dependency).

Large imports run for many minutes. With `--verbose`, the command streams a
running count of parsed concepts and stage-transition messages so progress is
visible; without it, the startup spinner covers the wait. If an import fails
partway through writing the store, it reports that the store may hold partial
versions of what it was importing and advises re-running; because content is
keyed by version, re-running with a corrected source repairs the store.

Peak memory does not grow with the number of concepts, but the largest
vocabularies still need more driver heap than the 1 GB default to hold the
working set of the Spark joins that build the store. The OMOP vocabulary (around
6.6 million concepts), for example, imports comfortably with a 4 GB heap. Set
the heap with `--spark-conf spark.driver.memory=4g`, or with the same key in the
[`[spark]` table](./configuration.md#spark-configuration) or the `SPARK_DRIVER_MEMORY` environment
variable.

```bash
pathling --spark-conf spark.driver.memory=4g import-fhir-terminology \
  /data/ohdsi.fhir.omop-0.1.0.tgz /data/tx-store
```

The `STORAGE_PATH` positional is optional: when it is omitted, the commands fall
back to the configured `tx-store.path` (see below). An explicit positional wins
over the configured path. Supplying neither is a usage error.

### Local terminology mode

Once a store has been populated, point any terminology-evaluating command at it
with `--tx-store` (or the `tx-store.path` config key) to evaluate terminology
against the local store instead of a remote server, entirely offline. This
applies to every command that creates a session, including `view`, `fhirpath`,
`convert`, `run`, and `console`.

```bash
pathling --tx-store /data/tx-store member-of codes.csv \
  --code-column code --system 'http://snomed.info/sct' \
  --value-set 'http://snomed.info/sct?fhir_vs=ecl/<404684003'
```

The [`[tx-store]`](./configuration.md#tx-store) config table records the store
once, along with optional tuning values:

```toml
[tx-store]
path = "/data/tx-store"                      # selects local mode
default-snomed-edition = "32506021000036107" # optional
expansion-cache-size = 200                   # optional, positive integer
default-dialect = "en-AU"                    # optional, used by import-snomed

[tx-store.dialect-aliases]                   # optional
en-NZ = "271000210107"
```

The presence of a store selects local mode. When a store is configured, the
store wins over any explicitly set `--tx-server` or terminology authentication:
each is ignored and a warning is printed. The built-in default server URL never
triggers this warning. A runtime failure in local mode names the store path and
suggests the import commands rather than a terminology server URL.
