# Implementation Plan: Non-conformant data reads as empty

**Feature**: `002-non-conformant-as-empty` | **Date**: 2026-11-24 | **Spec**: [spec.md](spec.md)

## Summary

Replace `trust` with one behaviour: conformant data gives correct answers (spec 001's FR-016 guarantee,
with a closed list of exceptions), and non-conformant content above level 1 never fails and is read as
empty. Conversions use Spark's safe (`try_`) forms, so the outcome does not depend on the session's ANSI
setting. Ingest plans a typed null per column where no cast reaches the declared type. Annotation kinds
and engine operations that parse a value use the null-returning parses of the shared grammar. Primitive
arrays drop positions that are no element, which the engine already does, and ingest pairs `x` with `_x`
over the longer array. Three questions are settled by spikes before the stories they gate: structure
emptiness (OQ-A), Parquet conformance and stored annotations (OQ-B), and where an invalid annotated value
becomes empty (OQ-C).

Tracking: #2884 is rescoped to this specification and #2921 is closed as folded into it. Work goes on
`issue/2884`, one PR per story or small group of stories, into `release/11.0.0`.

## Technical Context

**Language/Version**: Java 21; Python 3.10+ and R only for the documentation examples.
**Primary Dependencies**: Apache Spark 4 (SQL API), the existing `fhir-schema`, `io` and `fhirpath` modules.
**Storage**: Parquet and Delta in the PoF layout.
**Testing**: JUnit 5 in `io` and `fhirpath`, the FHIRPath DSL tests, the YAML conformance suites, all run under
both `spark.sql.ansi.enabled` settings as the harness does today.
**Target Platform**: Spark on the JVM: local, cluster and Databricks.
**Project Type**: library.
**Performance Goals**: no measurable regression on conformant data (SC-005). Structure emptiness (OQ-A) is
measured on the deepest and widest resource types of the benchmark corpus.
**Constraints**: `fhir-schema` and `io` may not use internal Catalyst API. No findings, logs or warnings
about non-conformant content (FR-015). FR-022 and FR-023 of spec 001: annotations stay a fast path,
decided from the schema.
**Scale/Scope**: every resource type; JSON, XML and bundles at ingest; Parquet and Delta at read.

## Constitution Check

Checked against the global, project and repository `CLAUDE.md` files and `CONTRIBUTING.md`.

- **Simplicity**: no modes, no reporting, no new ingest mechanism. The changes reuse what exists: the safe
  casts, `DateRangeKind`'s `…OrNull` parses, and the engine's existing element test for positions.
  Ticket 37's shape check comes back only as a per-column typed null, without findings. Pass.
- **Test-driven development**: every story starts with failing tests over the shared non-conformant corpus.
  Pass.
- **Language and framework rules**: Google Java style, `final`, nullability annotations, no inner classes,
  substantial logic kept out of function providers (`*Logic` classes), Optional and Stream composition.
  Pass.
- **Only strictly necessary code**: no switch, mode or option is added. The open questions are spikes with
  evidence, not speculative code. Pass.
- **Spec location**: tracked `specs/002-…`, with `evidence/` for the spikes, per `specs/README.md`. Pass.
- **Test-expectation approval**: changing YAML and FHIRPath-layer expectations needs owner approval. The
  tasks that touch `fhirpath-js/config.yaml` and DSL expectations say so. Pass.

No violations.

## Project Structure

### Documentation (this feature)

```text
specs/002-non-conformant-as-empty/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── checklists/requirements.md
├── evidence/          # Spike measurements for OQ-A, OQ-B and OQ-C.
└── tasks.md
```

### Source code (repository root)

```text
fhir-schema/src/main/java/au/csiro/pathling/schema/
├── annotation/kinds/DateRangeKind.java        # Annotation computed with the null-returning parse.
├── annotation/kinds/DecimalNumericKind.java   # try_cast already; documented as the decimal grammar.
└── date/FhirDateTime.java                     # The shared date grammar.

io/src/main/java/au/csiro/pathling/io/
├── ResourceTransformer.java                   # Per-column typed null on mismatch; safe casts; empty
│                                              # values absent; structure pruning (after OQ-A).
├── PrimitiveConverter(s).java                 # Safe conversions.
├── Annotator.java                             # repeatingGroup pairs over the longer array.
└── XmlIngestShape.java                        # The same rules for XML.

fhirpath/src/main/java/au/csiro/pathling/
├── fhirpath/comparison/TemporalComparator.java   # Null-returning bounds.
├── sql/misc/LowBoundaryForTime.java, HighBoundaryForTime.java
├── fhirpath/function/provider/ConversionLogic.java
├── fhirpath/encoding/QuantityEncoding.java
└── fhirpath/column/DefaultRepresentation.java    # Positions; structure emptiness (after OQ-A).

io/src/test/resources/nonconformant/            # The shared corpus: JSON and XML.
io/src/test/java/au/csiro/pathling/io/NonConformantParquetFixtures.java  # Writes the PoF Parquet cases.
io/src/test/java/au/csiro/pathling/io/NonConformantIngestTest.java
fhirpath/src/test/java/au/csiro/pathling/test/NonConformantQueryTest.java
fhirpath/src/test/java/au/csiro/pathling/test/NonConformantSchemaTest.java  # US6, after OQ-B.
site/docs/libraries/io/schema.md                # "Conformant and non-conformant data" section.
```

**Structure Decision**: the corpus and the Parquet fixtures live in `io`'s test sources. `io` gains the
`test-jar` goal and `fhirpath` a test-jar dependency on it (T002), since today `fhirpath` sees only `io`'s
main jar. No new module.

## Phase 0: Research

Recorded in [research.md](research.md): R1 safe casts, R2 the decimal grammar, R3 the per-column
mismatch plan, R4 mixed-batch divergence, R5 the shared corpus, R6 tracking, R7 the array rule already in
the engine. OQ-A, OQ-B and OQ-C are recorded there as open, with their spikes.

## Phase 1: Design & Contracts

- **Data model**: [data-model.md](data-model.md), the table of each content shape and what it reads as.
- **Contracts**: none. No public API changes. The behaviour contract is the documentation page (FR-019,
  FR-020).
- **Quickstart**: [quickstart.md](quickstart.md).

## Phases of delivery

1. Setup and foundation: the corpus and the safe-parse foundation (null-returning parses everywhere).
2. US1: documentation, timezone pins, spec 001 amendments.
3. US2 and US3: ingest rules and array rules, JSON and XML.
4. US4: engine over stored data from any writer. US4 scenario 4 waits for OQ-C.
5. Spikes: OQ-A, OQ-B and OQ-C, each producing evidence and an owner decision.
6. US5 after OQ-A, US6 after OQ-B.
7. Polish: conformance exclusions, benchmark check, documentation pass.
