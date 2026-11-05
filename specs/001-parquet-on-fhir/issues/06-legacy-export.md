# 06 — Legacy export: legacy Parquet/Delta to FHIR NDJSON, as a component and CLI

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-064, FR-051, SC-012, US9. Replaces T134a.

**What to build:** A user with a warehouse in the legacy layout can convert it to FHIR NDJSON, then re-import it through the PoF path. The export is a reusable component and a command-line tool, in a module of its own. It depends only on `encoders`, decoding with the legacy encoder, and is outside `library-runtime` and the language bindings. It reads legacy-layout Parquet and Delta tables, one resource type each, and writes NDJSON per type. Its losses are documented: for example, the legacy layout's fixed-scale decimals, and anything the legacy encoder did not store.

**Blocked by:** None — can start immediately

**Status:** done

- [x] The new module is not a dependency of `library-runtime`, Python or R. It depends on `encoders` and nothing newer in the core chain.
- [x] The component exports a legacy Parquet table and a legacy Delta table to NDJSON.
- [x] The CLI takes a source location, a format and an output location, and exits non-zero with a clear message on a non-legacy or unreadable source.
- [x] SC-012 test: a table written by the legacy encoder, exported and re-imported through `FhirReader`, holds every resource of the source, equal to it except for the documented losses.
- [x] The losses are listed in the module's documentation.
- [x] `encoders` is unmodified in its encoding behaviour (FR-051).

## Comments

### Implementation (branch `issue/2829-legacy-export`)

A new module, `legacy-export` (`au.csiro.pathling:legacy-export`, package `au.csiro.pathling.legacy`),
registered in the root `pom.xml` after `io`, added to the test matrices of `test.yml` and
`pre-release.yml`, and listed in `CONTRIBUTING.md`.

**Dependencies, for review.** The module depends on `encoders` at compile scope, which brings
`utilities`. Spark, Delta Lake, Hadoop and Scala are `provided`, as they are for `encoders`. It
also depends on `io`, but only at **test scope**, because the SC-012 round trip re-imports through
`FhirReader`. Test scope is not transitive and is not in the published POM's runtime
dependencies, so no consumer of the artifact sees `io`. This is the reading taken of "depends on
`encoders` and nothing newer in the core chain", and a reviewer should confirm it.
- `mvn dependency:tree -pl legacy-export`: `encoders` and `utilities` at compile scope, `io` and
  `fhir-schema` at test scope.
- `mvn dependency:tree` on `library-runtime`, `lib/python` and `lib/R`: none mentions
  `legacy-export`, and no POM other than the root's module list names it.
- `git diff issue/2829 -- encoders/` is empty (FR-051).

**Component.** `LegacyExport.of(spark)`:
- `toNdjson(resourceType, table)` turns a legacy table that has already been read into one FHIR
  JSON document per row.
- `read(path, LegacyTableFormat.PARQUET | DELTA)` reads a table.
- `export(resourceType, source, format, outputDirectory)` writes `<type>.<part>.ndjson` files,
  which is the naming the NDJSON source reads. It refuses an output that already holds that type's
  files.
- Every failure is a `LegacyExportException`.

**Refusals.** These are stricter than the ticket asked for:
- A table without a root string `id_versioned` beside `id` is not legacy.
- A table carrying `resourceType` as well is mixed.
- A table holding a column the decoder would not read is refused, with the column named.

The legacy decoder reads by name and silently ignores unknown columns (checked by experiment). The
table does not record its encoder configuration, so the decoder is set up as follows:
- extensions are enabled where the table has `_extension`;
- all open types are enabled;
- the nesting level is the lowest one, searched from 0 to 10, at which every column is read.

That rule is also what refuses a table of another resource type.

**CLI shape, for review.** `LegacyExportCommand`:

`<source> <parquet|delta> <output> [--resource-type <type>]`

- Exit codes are 0 for success, 1 for a failed export (non-legacy, mixed, unreadable, unread
  columns, or an output that already holds the type) and 2 for a usage error.
- With no `--resource-type`, a source named `<Type>.<anything>` is one table.
- Any other source is treated as a directory of such tables, which is beyond the ticket, so that a
  whole warehouse can be exported in one run. A directory that fails part-way leaves the types
  already exported on disk. The command prints each type it exports, and then exits 1.
- There is no CLI framework. Argument parsing is hand-written, following `test-data`'s plain
  `main`.
- Delta Lake is configured only for Delta sources. Without a master the command runs locally,
  otherwise under `spark-submit`.

**Packaging, open.** The jar is plain, not shaded, and Spark and Delta are provided.
- A smoke run with `spark-submit --jars <runtime classpath> --class
  au.csiro.pathling.legacy.LegacyExportCommand` exported a two-table warehouse.
- The `--packages au.csiro.pathling:encoders:<version>` form in the module documentation was not
  run, so that the shared Ivy cache was left alone.
- Whether to publish a runnable bundle is undecided.

**Losses.** These are listed in `package-info.java`. Each was confirmed by experiment and is asserted
by the round trip:
- **Decimals:** stored at scale 6, rounded half up, keeping the original scale up to 6. A value with
  more than 26 integer digits was not stored.
- **Instants:** exported in UTC to the millisecond.
- **Nesting:** an element nested in more than three elements of its own type was dropped.
  Extensions are exempt.
- **Extensions:** extension values outside the open types were not stored. Where extensions were
  disabled, none were stored.
- Modifier extensions were not stored.
- The ids and extensions of primitives were not stored.
- Contained resources were not stored. References to them dangle.
- `ValueSet.expansion.contains.contains` was not stored.
- `Bundle`, `Parameters`, `StructureDefinition` and `StructureMap` were never encoded.

**SC-012 test (`LegacyRoundTripTest`).**
- **The round trip:** Synthea `Patient`, `Condition`, `Observation` (200 rows) and
  `Questionnaire` are written by the legacy encoder at the library's old defaults, as Parquet and
  as Delta. They are exported, read through `FhirReader`, written as Parquet, read back and
  written as JSON through `FhirWriter`.
- **The comparison:** the result is compared semantically with the source after `DocumentedLosses`
  applies the documented losses. The test model is HAPI-based.
- **Per-loss fixtures:** fixtures under `data/losses/` are each asserted to carry a loss.
- **Controls:**
  - HAPI is asserted to round-trip the corpus unchanged, so a loss HAPI shares with the export
    cannot hide.
  - A mutation that removed the decimal rule from the model failed six cases.

**Review findings left open.**
- `LegacyTableFormat` is a top-level enum, as `StorageLayout` is. `CONTRIBUTING.md` says to
  avoid enums.
- `LegacyExport.read` catches `Exception`, because Spark's `AnalysisException` is checked but
  undeclared.
- Table discovery could move out of `LegacyExportCommand` into a class of its own.
