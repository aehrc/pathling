# Design: Parquet on FHIR

Umbrella design for replacing Pathling's Catalyst-encoder-based FHIR encoding with
a schema-driven encoding conforming to the
[Parquet on FHIR](https://github.com/aehrc/parquet-on-fhir) specification.

Tracks GitHub issue [#2367](https://github.com/aehrc/pathling/issues/2367).

This document is not a change. It is the cross-cutting design that several
changes under `openspec/changes/` implement, following the pattern of
`repeatAll-design.md`. Individual changes carry their own proposals, tasks and
specs and reference this document for context.

## Drivers

Five, in the order they motivate the work.

1. **Performance** of JSON encoding and decoding, and of FHIRPath execution,
   through reduced schema width. *Hypothesis — not yet measured. See
   "Verification gates".*
2. **Lossless representation of FHIR**, permitting a full round trip
   `JSON → SQL/Parquet → JSON`.
3. **Attribute-level extensions and ids**, and unconstrained or data-inferred
   depth for recursive types.
4. **Automatic merging** of Parquet and Delta files written with differing
   sparse schemas into a common schema. *Verified for Delta; conditional for
   raw Parquet. See "Verification gates" and "Open risks".*
5. **Removal of the dependency on internal Spark Catalyst API** in the encoding
   path. The current encoder is built from hand-authored Catalyst expression
   trees against internal, partly deprecated API, which breaks on Spark version
   upgrades. The equivalent dependency in the *execution* path is out of scope
   here and is separate future work.

Driver 5 is the reason the encoder is replaced rather than modified. Drivers 2
and 3 concern *what schema is emitted* and could in principle be met by changing
the existing encoder; driver 5 is what makes the mechanism itself the subject.

## Constraints

- **No internal Catalyst API in the encoding path.** Conversions are dataset
  transformations — Spark column expressions over a known schema — or UDFs.
  UDFs are public, stable Spark API, so HAPI *inside a UDF* is permitted; what
  is excluded is `ExpressionEncoder`, hand-authored serializer and deserializer
  expression trees, and dependence on `org.apache.spark.sql.catalyst`.
- **The execution path is out of scope.** `StructProduct`, `IfArray`,
  `TransformTree`, `RowIndexCounter`, `TraceExpression` and the rest of the
  query-time toolkit remain custom Catalyst expressions. They are relocated,
  not rewritten.
- **The conventions change in this programme.** The scope line between this work
  and the later execution work is a *dependency* line, not a concern line. Every
  encoding convention that changes has a query-side consumer, and those
  consumers change here (see "Convention consumers").

## Approach: definition-derived, data-pruned schemas

The FHIR schema is known for each element and determines the SQL schema. A
schema is produced by walking FHIR structural definitions to obtain **types and
cardinality**, then optionally **pruning** the resulting `StructType` to the
elements actually present in the data.

```
  FHIR definitions ──walk──▶ full StructType ──prune to data──▶ sparse schema
                                     │
                                     └── no pruning ──────────▶ dense schema
```

The dense schema is the un-pruned degenerate case of the same builder, not a
second mechanism. This matters: the two cannot drift apart, and testing one does
not require re-testing the other from scratch.

**Why not infer the schema from the data**, as the `spike/parquet-on-fhir`
prototype does:

- A repeating element present once in one batch and twice in another infers as
  a struct versus an array of structs. Merging those fails at planning with
  `CANNOT_MERGE_SCHEMAS`. Deriving cardinality from definitions removes the
  conflict by construction.
- JSON inference widens incompatible types to `STRING` rather than failing, so
  a schema silently degrades instead of erroring.
- `Patient.bogusField` is indistinguishable from an element absent from this
  dataset. With a definition-derived schema, "in the definitions but not in this
  schema" means absent from the data, and "not in the definitions" is a
  modelling error.

**Definition source.** Structural metadata is obtained through the
`DefinitionContext` abstraction, which already exists in `fhirpath` with two
implementations — `FhirDefinitionContext` (backed by HAPI's reflected R4 model)
and `DefaultDefinitionContext` (a plain map). The interface is currently a
name-lookup facade and must be widened to expose child enumeration and
cardinality before it can drive schema derivation.

A `StructureDefinition`-backed implementation — which would make profiles and
non-R4 versions loadable as data — is a **non-goal** of this programme. The
interface is de-R4'd so that such an implementation can be added later without
breaking it.

## Losslessness contract

**Conformant-input semantic losslessness.** For input conforming to the active
definition set, `JSON → storage → JSON` produces a semantically equal resource:
object key order ignored, array order significant, numbers compared **lexically**
(which is why decimals are stored as `STRING`).

- **Non-conformant input** — content not described by the definition set — is
  governed by a configurable switch: ignore, or fail with a diagnostic. Silent
  truncation is not an option.
- **`contained` resources are excluded.** They are not represented today either
  (`SchemaTraversal` returns `Nil` for `RuntimeChildContainedResources`), so this
  is parity with the current encoder, but it is *not* parity with losslessness.
  The presence of `contained` is a detected condition governed by the same
  ignore/fail switch, so the carve-out is never silent.
- **On the pruned schema the guarantee is unconditional.** Depth comes from the
  data, extensions appear because they are present, open types resolve as
  observed.
- **On the dense schema the guarantee is bounded by configuration.**
  `enable_extensions` (default `false`), `max_nesting_level` (default `3`) and
  `enabled_open_types` each drop content by design. In fail mode, content those
  bounds would drop is an error rather than a silent truncation.

The claim to make in documentation is therefore: *lossless round trip for
conformant input on the pruned schema; on the dense schema, lossless within the
configured nesting, extension and open-type bounds, with violations detectable.*

Losslessness is verified by a round-trip harness over a real corpus, not
asserted. See "Verification gates".

## Adoption of the Parquet on FHIR specification

The specification is adopted with the deviations below. All are permitted by the
specification itself, which requires only that `resourceType` be present and
states that consuming applications **SHALL** tolerate the absence of any other
field.

| Area | Decision |
|---|---|
| Decimals | `STRING` lexical form plus the spec's `__x_numeric` annotation. |
| Primitive extensions and ids | The spec's `_field { id, extension }` groups. Currently unrepresentable — `_fid` is per composite and a scalar column has nowhere to hang one. |
| Extensions | Inline `extension` groups, replacing the root-level `MAP<INT, Extension>` keyed by `_fid`. |
| Date ranges | The spec's `__x_start` / `__x_end` annotations. |
| Quantity canonicalisation | **Deviation.** The spec's `canonical` annotation is *not* emitted. Its `value` field is typed `DECIMAL(38, 6)`, a fixed-point type whose absolute precision is 1e-6 regardless of magnitude, while canonicalisation shifts magnitude by arbitrary powers of ten. A mass of `1 ng` canonicalises to `0.000000001 g` and stores as `0.000000`, so quantities differing by orders of magnitude compare equal — silently. Pathling emits its own wider annotation under a non-colliding name, carrying `FlexiDecimal`'s arbitrary scale. Non-standard annotations are explicitly permitted. Raised upstream as [aehrc/parquet-on-fhir#1](https://github.com/aehrc/parquet-on-fhir/issues/1); if the specification adopts a representation that preserves magnitude, this deviation is withdrawn. The spec form can be added later additively if an interchange consumer needs it. |
| `contained` | Excluded, as above. |

**Annotations are optional in the specification**, so any conformant file may
arrive with none. The FHIRPath engine therefore **must compute from the lossless
source when an annotation is absent**, using the annotation only as a fast path
when present. An engine that requires `__x_numeric` to compare decimals can only
read files Pathling itself wrote, which defeats adopting the format.

## Module structure

`encoders` currently holds two unrelated concerns plus a schema builder that
belongs to neither. The target is three modules:

```
utilities → fhir-schema → encoders → terminology → fhirpath → library-api
            │             │
            │             └─ HAPI ↔ Row bridge; shrinks toward zero over
            │                the sequence
            └─ FHIR definitions, schema derivation, Catalyst-free HAPI
               utilities. Depends on spark-sql-api, never on spark-catalyst.

            spark-toolkit ─ query-time Catalyst expressions (StructProduct,
                            IfArray, TransformTree, RowIndexCounter,
                            TraceExpression, …). Consumed by fhirpath.
```

The `encoders` module is **not renamed**; it shrinks in place, so existing
`-pl encoders` invocations and Maven coordinates keep working.

`DefinitionContext` and the rest of `au.csiro.pathling.fhirpath.definition` move
down into `fhir-schema` and are renamed to `au.csiro.pathling.definition`. No
module outside `fhirpath` consumes the package today, so the move is an import
sweep within one module.

**The schema builder cannot move in the first change.** `SchemaConverter`
imports `DataTypeMappings` and extends `EncoderContext`, both of which reach
Catalyst; `FlexiDecimal` reaches it through `DecimalCustomCoder`;
`EncodingConfiguration` through `FhirEncoders`. Severing the `CustomCoder`
schema hook is a behavioural change, so it belongs to the schema-builder change,
not to the pure-motion split.

## Convention consumers

Every convention that changes has query-side code that reads it. These change in
this programme, not in the later execution work.

| Convention | Consumers |
|---|---|
| `_scale` → `__x_numeric` | `DecimalCollection`, `FlexiDecimalSupport` |
| `_value_canonicalized` → wide canonical annotation | `QuantityEncoding`, `QuantityMatcher` |
| `_fid` / `_extension` → inline groups | `Collection.getFid()`, `ResourceCollection` |
| `id_versioned` | reference resolution and the cross-resource join machinery |
| `CodingSchema` 7-field struct | every terminology UDF — `member_of`, `translate`, `subsumes`, `display`, `designation`, `property`, all public API in Python and R |

`CodingSchema` is the sharpest edge in the programme. It is decoded **by field
index**, so under a sparse schema every dropped field shifts the indices after
it and `CODE_INDEX` reads past the end — silently, not loudly. Converting it to
name-based access is correct under both the old and new layouts and is sequenced
early, independently of the conventions themselves.

## The sequence

Each item is a separate change under `openspec/changes/`, proposed when its
prerequisites are settled rather than all up front. Items 1 to 5 are proposed;
items 2 to 5 depend on nothing else in the sequence and can proceed in parallel
with item 1.

| # | Change | Notes |
|---|---|---|
| 0 | Capture JMH baseline on `main` | Prerequisite for driver 1. Must precede any module motion. |
| 1 | `split-encoders-module` | Pure motion, no behaviour change. Lands first. |
| 2 | `coding-schema-by-name` | Defensive; correct under both layouts. |
| 3 | `source-schema-detection` | `ParquetSource` / `DeltaSource` / `CatalogSource` reject unknown layouts with an actionable error. Correct against today's code. |
| 4 | `reference-join-coverage` | Portable JSON coverage for the join machinery, before its conventions change. |
| 5 | `definition-context-widening` | Child enumeration, cardinality, de-R4 the interface. |
| 6 | `fhir-schema-builder` | Sever the `CustomCoder` hook; the builder moves to `fhir-schema`; pruning. |
| 7 | `pof-schema-conventions` | The layout itself, plus the convention consumers above. |
| 8 | `json-ingest` | Category A, including `Dataset<String>` and bundles. |
| 9 | `json-egress` | Category B. Unblocks the server's `$export`. |
| 10 | `object-roundtrip` | Categories C and D. Server-side. |
| 11 | `persisted-data-migration` | Category F, version gate and CLI tool. Server-side. |

Items 10 and 11 are the server catch-up sub-sequence (see "Release
coordination").

## Test strategy

- **Sparse schema by default** in functional tests; dense is opt-in per test via
  an annotation, declared at the test rather than in a central list. The default
  is switchable from Maven with `-Dpathling.testSchemaMode=dense`.
- **The mode switch is self-verifying.** One test asserts that the active mode
  matches the requested one and fails loudly otherwise. This repository has
  precedent for test configuration that silently does nothing (`exclusionsOnly`,
  an exclude block's `glob`, a rule's `desc`); a mode switch that fails quietly
  would leave a green build in a mode nobody is running.
- **CI runs sparse everywhere plus a curated dense subset** — schema
  construction, round-trip, cardinality and missing-field handling. Not the full
  suite twice: around 90% of a core build is test execution, and the dense
  schema is the same code path with pruning skipped, not a second
  implementation. The full dense run stays available on demand.
- **Shape-sensitive tests assert their schema explicitly.** A sparse schema is
  derived from the whole fixture set, so adding a fixture to a test method can
  silently flip an assertion from a null branch to a missing-field branch.
  Asserting the expected `treeString()` catches that.
- **HAPI fixtures reach Spark through the category-C path** — HAPI object → JSON
  → read with an explicit derived schema. The production `ObjectDataSource` and
  its verbatim duplicate in `fhirpath/src/test` share a mechanism, so fixing one
  fixes the other and the fluent builders need not be rewritten.

## Compatibility and migration

**Data at rest is breaking.** Existing tables carry `_fid`, `_extension`,
`_scale`, `id_versioned` and canonicalised quantity fields, and store decimals
as `DECIMAL(38,6)` where the target is `STRING`. `SchemaMigrator` handles only
additive drift and states in its own Javadoc that a table carrying fields the
encoders no longer emit cannot be migrated. The decimal change is not additive
at all: Delta raises `DELTA_FAILED_TO_MERGE_FIELDS` on the type conflict, and
only `overwriteSchema` — a destructive replace — gets past it. There is no
incremental evolution path into this layout.

- **Default: a version gate.** Deployments are detected and told to migrate.
- **Opt-in: a CLI rewrite tool**, built from the existing encoders retained as
  deprecated classes in a **separate migration artifact**, not in
  `library-runtime`. Old Row → HAPI → JSON → new layout, reusing machinery that
  already works. The same tool points at a Parquet directory for library users.
  The tool has a shelf life — it contains exactly the code whose Spark-version
  fragility motivates driver 5 — so the supported statement is "migrate at or
  before version X".
- **Migration is faithful, not lossless.** The old layout already truncated
  decimals past `DECIMAL(38,6)` and already dropped `contained`. A migrated
  warehouse does not carry driver 2's guarantee; a re-imported one does.
- **Detection everywhere.** No source validates the stored schema on read today,
  so an old file flows straight into an engine expecting new conventions.
  Detection with an actionable error is sequenced early and is correct against
  the current codebase.

**Public API is non-breaking.** `encode(Dataset<String>)` and `encodeBundle` are
preserved in Java, Python and R. `max_nesting_level`, `enable_extensions` and
`enabled_open_types` keep their current semantics for the dense schema and do
not apply to the pruned one — no deprecation, no reinterpretation.

**Capability coverage is preserved.** XML ingest and Bundle ingest are *not*
regressions. HAPI remains in the build — `FhirDefinitionContext` is HAPI-backed,
and `FhirConversionSupport`, `R4FhirConversionSupport` and `FhirTraversal`
survive — so both are handled by parsing with HAPI in a UDF and handing JSON to
the ingest path. Bundles are supported as **data carriers**: transport only,
exploded to per-type tables, never a stored resource type.

`site/docs/libraries/io/schema.md` is the published contract for the current
layout and becomes a rewrite pointing at the Parquet on FHIR specification. It
is where any compatibility statement for existing files belongs.

## Release coordination

The server pins to the last pre-change library release and catches up in one
migration covering categories C, D, E and F, plus the 41 server test files
carrying the encoder dependency. The library sequence is not gated on server
work, so drivers 2, 3 and 5 reach library users sooner.

Consequences to manage:

- **Pin point.** The server tracks the pure-motion changes with import sweeps —
  `ViewResolver`, `SubjectResolver` and `FhirServer` import `ResourceTypes` and
  `ViewDefinitionResource` from `au.csiro.pathling.encoders`, and those move —
  and pins at the last release **before the first convention change** (item 7).
- **Cap the pin.** Name the library version the server must reach by, rather
  than leaving it open-ended. The longer the pin, the larger the catch-up.
- The pinned library release becomes a support point; server-side fixes may need
  library backports to it.
- The deprecated-encoders migration artifact stays load-bearing for the duration.

## Verification gates

| Gate | Instrument | Blocks |
|---|---|---|
| Driver 1 — performance | JMH `PathlingBenchmark`. Baseline captured on `main` **before** any module motion. Encode, decode and query execution timed **separately** — the current seven benchmarks are shaped NDJSON → ViewDefinition and report one number, which cannot answer the question either way. Planning time measured separately from execution time. | Nothing. If the measurement is flat, driver 1 becomes a non-goal and the programme stands on drivers 2, 3 and 5. |
| Driver 2 — losslessness | Round-trip harness over a real corpus (FHIR R4 spec examples for coverage, Synthea for volume), asserting semantic equality resource by resource. Must include `1.50`, `1e2`, `1.0e-7`, a 40-digit decimal and a leading-`+` value. | Items 7 onward. |
| Driver 4 — sparse merging | Verified for Delta (see appendix). For raw Parquet, measure `mergeSchema` cost over a realistic file count on object storage. | Item 6's pruning design if raw Parquet proves impractical at scale. |
| Module boundary | `fhir-schema` must not depend on `spark-catalyst`. Enforced by build configuration, not convention. | Item 1. |

Driver 1 is stated in the proposal as *expected to improve ingest and
query-planning cost through reduced schema width; execution-time effects
unquantified pending measurement* — not as a flat performance claim.

Note on where the driver-1 win is likely to be: Parquet is columnar, so unread
columns are not read and a dense schema costs little at *scan* time. The
plausible wins are in Catalyst analysis and planning over very large
`StructType`s, in ingest (materialising and writing all-null columns), in Delta
log and footer size, and in schema-merge cost. A benchmark that times only query
execution over scanned data will likely show nothing.

## Non-goals

- Removing internal Catalyst API from the **execution** path. Separate future
  work.
- A `StructureDefinition`-backed `DefinitionContext`. The interface is prepared
  for it; the implementation is a separate proposal motivated by profile
  support.
- Emitting the specification's `canonical` annotation for interchange. Additive
  later if a consumer needs it.
- Representing `contained` resources.
- Storing `Bundle` as a resource type.
- Renaming the `encoders` Maven module.

## Open risks

**Nested schema pruning can drop rows on sparse files.** The Parquet reader
reconstructs array offsets from leaves it actually requests. On a file lacking a
given leaf, `explode(name)` selecting *only* that leaf makes the row disappear
entirely; adding any sibling column makes it reappear. In SQL-on-FHIR terms, a
`forEach: name` view with a single `prefix` column loses resources from batches
that never carried that element. Reproduced on Delta as well as raw Parquet, so
it attaches to physical file sparsity rather than to the logical schema. No JIRA
identified; **needs independent reproduction before it is relied upon as fact**.
Known blunt mitigation is `spark.sql.optimizer.nestedSchemaPruning.enabled=false`,
at the cost of reading every leaf of every touched struct. An unverified
candidate worth testing: guarantee that every struct in a pruned schema retains
at least one leaf that is physically written in every file, so offsets remain
reconstructible.

**Decimal lexical form is not preserved on every ingest path.** Storing decimals
as `STRING` means the derived schema declares a `StringType` target where the
JSON carries a number token. Spark's `JacksonParser` preserves the raw text
byte-exactly only for some input streams: file-based JSONL (plain or compressed)
and uncompressed multiline are byte-exact; **gzipped multiline, `from_json`, and
`spark.read().json(Dataset<String>)` re-serialise**, turning `1.50` into `1.5`
and `1e2` into `100.0`. `PathlingContext.encode` takes a `Dataset<String>` and is
therefore on the re-serialising path, as is the category-C mechanism used by
every server write. A mechanism is required; candidates are recorded in the
appendix. Verified by the driver-2 harness and benchmarked under the driver-1
gate.

## Appendix A: Spark and Delta findings

Verified against Spark 4.0.2 and Delta 4.0.0 by source reading plus experiment.

**Sparse schema merging (driver 4).**
`StructType.merge` recurses at every level — `StructType`, `ArrayType` element
types and `MapType` keys and values. Delta's `SchemaMergingUtils.mergeDataTypes`
does likewise; an append with `mergeSchema` was verified adding new fields five
levels deep inside an `array<struct>` in a single commit. The Delta
documentation and a stale comment in that file both claim arrays are
unsupported; both are out of date. Delta reads its schema from the log, so no
footer scan and no file listing. Raw Parquet `mergeSchema` is equally recursive
but reads every footer, is driver-coordinated, and is **off by default**;
without it the lexicographically first data file's schema wins and the rest are
null-filled. Type conflicts fail loudly everywhere —
`CANNOT_MERGE_SCHEMAS` at planning for Parquet, `DELTA_FAILED_TO_MERGE_FIELDS`
for Delta writes.

**JSON parsing.**
`JacksonParser` handles a `StringType` target over a non-string token by
recording the token location, skipping children, and branching on the stream
type: byte-exact when the raw content is a byte array or positionally readable,
re-serialised otherwise. `spark.sql.json.enableExactStringParsing` defaults true
but is internal, and the branch is decided by stream type rather than by the
config. `from_json` and `spark.read().json(Dataset[String])` wrap bytes in a
reader and therefore always re-serialise. Re-serialisation normalises numbers
and folds escapes.

`from_json` with an explicit `StructType` **silently skips unknown fields in
every mode, including `FAILFAST`** — there is no strict mode. The losslessness
switch's fail behaviour must therefore be implemented by comparing observed keys
against the definitions, not by asking Spark to enforce it. Separately,
`spark.sql.json.enablePartialResults` (default true, internal) means a type
mismatch under `PERMISSIVE` nulls only the offending field, so schema drift
degrades silently one field at a time.

A declared schema that is a strict subset of the JSON works and is tolerated; a
subtree is fully tokenised but not materialised, so a cheap `resourceType` probe
costs lexing, not object construction.

**VARIANT.** Supported in a read schema including nested inside
`array<struct<…>>`, castable directly to a declared `StructType` with extra keys
ignored and `try_cast` nulling field-by-field. Writable to Parquet and to Delta
4.0.0, though a Delta table gaining a variant column moves to reader 3 / writer 7
and becomes unreadable by clients lacking the feature. `to_json` on a variant
sorts keys and normalises numbers, so it does not preserve lexical form.

## Appendix B: What the prototype does

`spike/parquet-on-fhir` (commit `8891777ca1`) does **not** derive a schema from
FHIR definitions. Spark's JSON inference produces the schema; `DatasetTransformer`
walks that inferred Spark schema and asks HAPI one question per observed field
name — "what FHIR datatype is this?" — to key a small set of value transforms.
Its entire HAPI surface is `getResourceDefinition`, `getChildByName` (two
overloads), `getName`, and one `instanceof`. `getMax()` is never called;
cardinality comes from `ArrayType`. Choice types are never enumerated.

It is therefore a working demonstration of schema *inference*, which this design
rejects. What transfers is the convention work — the `__x_numeric` annotation,
the JSON read and write wiring, the transform-map shape — not the traversal,
which runs schema-first where this design runs definitions-first.

Two specifics worth carrying forward as anti-patterns: it drops `contained` by
string match at top level only, and it silently passes through any field HAPI
does not recognise, which is the behaviour the ignore/fail switch replaces.

## Appendix C: Decision register

| # | Decision |
|---|---|
| 1 | Driver 5 is removal of internal Catalyst API dependency in the encoding path; the execution path is separate. |
| 2 | A sequence of changes under this umbrella, not one change. `split-encoders-module` first. |
| 3 | Dependency line, not concern line — conventions and their query-side readers change together. |
| 4 | Definition-derived, data-pruned schemas; dense is the un-pruned degenerate case. |
| 5 | Conformant-input semantic losslessness with a configurable ignore/fail switch; `contained` excluded. |
| 6 | The definition abstraction moves down into `fhir-schema`. |
| 7 | Widen the `DefinitionContext` seam only; no `StructureDefinition` provider in this programme. |
| 8 | Data at rest: version gate by default, opt-in CLI rewrite tool, detection everywhere. |
| 9 | `max_nesting_level`, `enable_extensions`, `enabled_open_types` keep dense semantics, do not apply to pruned. |
| 10 | Losslessness unconditional on pruned, bounded-by-configuration on dense, violations detectable. |
| 11 | No spec `canonical` annotation; a wider non-colliding one. The engine computes when an annotation is absent. |
| 12 | Tests reach Spark via the category-C path; portable reference-join coverage lands before reference conventions change. |
| 13 | Sparse by default in tests, dense opt-in, mode switchable and self-verifying; CI runs sparse plus a curated dense subset. |
| 14 | Driver 1 is a hypothesis with a measurement task; nothing is gated on it. |
| 15 | Bundles are data carriers, never stored; XML via HAPI-to-JSON. |
| 16 | `encode(Dataset<String>)` preserved; decimal lexical form on that path is an open risk with gates. |
| 17 | The server pins to the last pre-change library release, then catches up in one migration. |
