# Implementation Plan: Parquet on FHIR

**Feature**: `001-parquet-on-fhir` | **Date**: 2026-09-14 | **Spec**: [spec.md](spec.md)

## Summary

Replace Pathling's Catalyst-encoder-based FHIR encoding with a definition-derived,
data-pruned schema conforming to the Parquet on FHIR specification, and migrate
the FHIRPath execution engine and its dependencies to read that layout.

The approach is **additive**. A new `fhir-schema` module holds the FHIR
definition abstraction and schema derivation; a new `io` module holds the new
encoding as dataset transformations. The existing `encoders` module is left
untouched, retaining both the HAPI bridge and the query-time Catalyst toolkit, so
that migration tooling can still use it and this work adds rather than
restructures. On completion the engine reads only the new layout.

Ingest reads JSON with an inferred schema and transforms it into the
definition-derived target with SQL. Decimals are stored lexically as text;
lexical form is not preserved on the path that takes resources as a dataset of
strings, which is a Spark defect, reported upstream and documented.

## Technical Context

**Language/Version**: Java 21, Scala 2.13 (existing Scala left in place)
**Primary Dependencies**: Apache Spark 4.0.2, Delta 4.0.0, HAPI FHIR (R4)
**Storage**: Parquet and Delta, in the Parquet on FHIR layout
**Testing**: JUnit 5, the FHIRPath DSL test framework, the YAML conformance
runners, the SQL-on-FHIR compliance suite, JMH for benchmarks
**Target Platform**: JVM on Spark clusters; Python and R bindings via
`library-runtime`
**Project Type**: Library (multi-module Maven), plus language bindings
**Performance Goals**: None required. Encode, decode and query execution are
measured separately against a pre-change baseline, with planning time separated
from execution time. Driver 1 is a hypothesis; nothing is gated on the outcome.
**Constraints**: No internal Spark Catalyst API in the new encoding path, nor in
the definitions and schema derivation. The evaluation path is exempt and is
extended: tolerant traversal and shape reconciliation are added to the
query-time expression toolkit. No change to the semantics of the public library
API, including that an expression still becomes a column with no dataset in
hand. No modification to the existing encoders.
**Scale/Scope**: All R4 resource types; the full FHIRPath and SQL-on-FHIR test
estate (39 files construct FHIR objects directly, 67 reference the encoder
handle).

## Constitution Check

The constitution is the user's `CLAUDE.md` files: the global profile rules, the
repository `CLAUDE.md` chain (`CONTRIBUTING.md`, `server/CONTRIBUTING.md`,
`ui/CONTRIBUTING.md`) and `.claude/CLAUDE.md`.

- **Simplicity** — the plan adds two modules and moves one package, rather than
  restructuring `encoders` as the superseded design proposed. Deferring that
  split removes a large pure-motion change from the critical path and keeps the
  old implementation available to migration tooling. The one remaining piece of
  motion, the definition package, is required because schema derivation must sit
  below the engine.
- **Test-driven development** — mandatory, and every behavioural task in
  `tasks.md` is preceded by its test task. Two categories need justification:
  - the definition package move is pure motion, where *the existing suite is the
    test*: the requirement is that no test changes other than its imports, which
    is verified by reviewing the test diff;
  - the coverage-first work (portable reference-join fixtures) is tests only, by
    design, and lands before the conventions it protects.
- **Java conventions** — Google Java Style, `final` on parameters and locals,
  nullability annotations, no inner classes, records or enums, logging rather
  than `System.out`, no `TODO` in submitted code. Substantial logic goes in a
  dedicated `*Logic` class rather than inline in a provider, as
  `ConversionFunctions` does with `ConversionLogic`. `Optional` and `Stream`
  composition are preferred over imperative null checks and loops, with chains
  broken after three or four calls.
- **Formatting** — `spotless:check` runs ahead of compilation, so
  `mvn spotless:apply -pl <module>` precedes every build.
- **Comments** — complete sentences, terminated with a period, explaining intent
  rather than restating code.
- **Commit messages** — `<type>: <objective>`, describing why rather than which
  files.
- **Branching** — issue branch `issue/2367` off `main`; the programme lands as
  several pull requests against it. Every milestone ends green and none runs red
  in the middle, so there is no span that has to land as one unreleasable piece.
  Three independent axes are what that rests on — absence, conventions and
  density — and the test framework carries the latter two as dimensions
  (decision 52).

| Violation | Why needed | Simpler alternative rejected because |
| --- | --- | --- |
| The new encoding duplicates schema derivation that `SchemaConverter` performs today | `SchemaConverter` reaches Catalyst through the `CustomCoder` schema hook, and the existing encoders must not be modified | Reusing it in place would either require modifying `encoders` (excluded by decision) or carrying its Catalyst dependency into the new module (excluded by FR-048) |
| Two layouts exist in the build for the duration | The old encoders are retained for migration tooling | Retiring them now would leave no path for users to migrate data at rest |
| The query engine reads both layouts from M2 until T100e removes the previous reader in M6 | It is what lets the engine be rewritten behind a green build and makes the switch a writer flip rather than a migration; it also keeps the switch reversible until M6 | Converting the engine by replacement was the previous plan and produced a span with no green build at all (decision 40, reversed). The cost is recorded as decision 51 and retired by a named task rather than left open |
| FHIR objects still appear inside a UDF for XML and Bundle ingest | Neither has a Spark-native path that preserves FHIR semantics | A second parser for each would split the codebase permanently; UDFs are public, stable Spark API, so the constraint is met |

## Project Structure

### Documentation (this feature)

```text
specs/001-parquet-on-fhir/
├── spec.md
├── plan.md            # This file.
├── research.md        # Phase 0 output.
├── data-model.md      # Phase 1 output.
├── quickstart.md      # Phase 1 output.
├── contracts/
│   ├── storage-layout.md
│   ├── library-api.md
│   └── engine-semantics.md
├── checklists/
│   └── requirements.md
├── evidence/               # The measurements the decisions rest on.
│   ├── spark-type-findings.md
│   ├── decisions.md
│   ├── encoder-scope.md
│   └── scripts/           # Reproductions for every measurement.
└── tasks.md
```

### Source code (repository root)

```text
utilities/                          # Gains the structure merge and the
  src/main/java/au/csiro/pathling/utilities/       # canonical structure
                                      # interface, so that both `encoders` and
                                      # `fhir-schema` reach them without either
                                      # depending on the other.

fhir-schema/                        # NEW. spark-sql-api only; no Catalyst.
  src/main/java/au/csiro/pathling/definition/      # Moved from fhirpath.
  src/main/java/au/csiro/pathling/schema/          # Derivation, pruning, and
                                      # the definition-backed canonical
                                      # structure that derivation and merging
                                      # share.

io/                                 # NEW. The new encoding.
  src/main/java/au/csiro/pathling/io/transform/    # JSON-shaped <-> PoF
                                      # datasets, both directions.
  src/main/java/au/csiro/pathling/io/json/         # JSON text in files or
                                      # a Dataset<String> (decision 70).
  src/main/java/au/csiro/pathling/io/annotation/   # Annotation processors.

encoders/                           # Implementation unchanged; retained for
  src/main/scala/au/csiro/pathling/sql/   # migration tooling. Gains two
                                      # query-time expressions (FR-052).

terminology/
  src/main/java/au/csiro/pathling/fhirpath/encoding/CodingSchema.java

fhirpath/
  src/main/java/au/csiro/pathling/fhirpath/collection/   # Convention consumers.
  src/main/java/au/csiro/pathling/fhirpath/column/       # Traversal, siblings.
  src/main/java/au/csiro/pathling/projection/            # Output typing.
  src/main/java/au/csiro/pathling/search/filter/         # Field-name constants.

library-api/
  src/main/java/au/csiro/pathling/library/PathlingContext.java
  src/main/java/au/csiro/pathling/library/io/source/     # Layout detection.
  src/main/java/au/csiro/pathling/library/io/sink/       # Schema merging.

lib/python, lib/R                   # Binding surface for new options.
site/docs/libraries/io/schema.md    # The published layout contract.
benchmark/                          # JMH baseline and comparison.
```

**Structure Decision**: Additive. Build order becomes
`utilities -> fhir-schema -> {encoders, io} -> terminology -> fhirpath -> library-api`,
with `fhirpath` depending on `fhir-schema` and `encoders`, and on `io` from the
milestone whose engine tests first need data in the new layout (decision 67). `encoders` is
neither renamed nor split, and its encoding implementation is not modified — it
gains only two query-time expressions, which FR-052 permits — so `-pl encoders`
and its published
coordinates keep working and migration tooling retains its implementation. The
only motion is `au.csiro.pathling.fhirpath.definition` moving down into
`fhir-schema` as `au.csiro.pathling.definition`; no module outside `fhirpath`
consumes that package today, so it is an import sweep within one module.

## Phase 0: Research

Every open technical question was resolved during the interview, several of them
empirically. Decisions, rationale and rejected alternatives are recorded in
[research.md](research.md), and the raw measurements in
`evidence/spark-type-findings.md`.

The questions resolved were: how the engine is reconciled with a schema fitted
to the data, and therefore absent-element representation and result typing;
how collections of one FHIR type but differing shape are combined;
whether nested schema pruning can drop rows and whether Pathling is exposed;
lexical decimal preservation and whether any Spark option or the variant type
can provide it; the schema-merging behaviour of sinks and sources; how reference
keys survive the loss of the stored versioned-key column; how the engine reaches
primitive metadata and annotations; and where the new code lives.

## Phase 1: Design & Contracts

- **Data model** ([data-model.md](data-model.md)) — the stored layout: how each
  FHIR type maps to a Spark type, what annotations accompany it, how primitive
  metadata is carried, how pruning decides what survives, and the rules the
  round trip depends on.
- **Contracts** ([contracts/](contracts/)) — three, because this work has three
  distinct consumers:
  - `storage-layout.md`: what is written, for anyone reading the files with
    their own SQL. This is the contract that replaces the published schema
    documentation.
  - `library-api.md`: the public API in Java, Python and R — what is preserved,
    what options are added, and what behaviour changes.
  - `engine-semantics.md`: what FHIRPath evaluation guarantees over the new
    layout, including absent elements, annotation-free files and result types.
- **Quickstart** ([quickstart.md](quickstart.md)) — the end-to-end scenarios that
  exercise the feature, phrased so they can be run as validation.
