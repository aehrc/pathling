 # Encoder Replacement Scope

**Pathling · issue #2367 — Implement Parquet on FHIR**

What the current Catalyst encoder holds, what depends on it, and what each
dependency costs once the target solution is limited to schema transformation
and UDFs.

| | |
|---|---|
| Repo | `aehrc/pathling` @ `main` |
| Spark | 4.0.2 |
| Reference | `spike/parquet-on-fhir` (2024-11-10, forked from main 2024-05-22) |

---

## The constraint this scope assumes

The target solution uses **no custom Catalyst encoders**. Every conversion is
either a **dataset transformation** — Spark column expressions applied against
an inferred or derived schema — or a **UDF**. There is no `ExpressionEncoder`,
no serializer or deserializer expression tree, and no HAPI object in any
per-row Catalyst plan.

That constraint is what makes this scopeable. It rules out a middle path where
the encoder survives for awkward cases, and it forces every current use of
`FhirEncoders` to be re-expressed or retired.

---

## Four dispositions

Every dependency below carries one. Ordered by cost, not by likelihood —
*Relocate* is nearly free and *Replace* is where the project actually lives.
*Retire*, *Relocate* and *Rework* describe classes; *Replace* describes
capabilities, so it appears only in the feature-level inventory.

| Disposition | Meaning |
|---|---|
| **Relocate** | Survives unchanged. Sits in the wrong module today and needs to move, nothing more. |
| **Rework** | Survives in concept. The class stays but its output or its field conventions change. |
| **Retire** | Deleted. Exists only to bridge HAPI objects and Catalyst rows. |
| **Replace** | The capability is still needed but has no equivalent under the constraint. New mechanism required. |

---

## Headline: the module is two things

`encoders` contains 41 source files across two unrelated concerns.
`Expressions.scala` alone declares 16 custom Catalyst expressions, plus two
supporting traits (`UnevaluableCopy`, `UnresolvedFallbackIfMissingField`), and
they split cleanly:

**Concern one — HAPI ↔ Row machinery** (dies with the encoder, as intended)

```
StaticField          GetHapiValue        ObjectCast
RegisterFid          AttachExtensions
SerializerBuilder    DeserializerBuilder
```

**Concern two — query-time Catalyst toolkit** (needed no matter how rows arrived)

```
StructProduct                        UnresolvedUnnest
UnresolvedIfArray / IfArray2         UnresolvedTransformTree
UnresolvedNullIfMissingField         UnresolvedVariantUnwrap
UnresolvedEmptyArrayIfMissingField   RowCounterGet / Increment / Reset
```

The right column is load-bearing query machinery:

- `StructProduct` is how `ProjectionResult` performs view unnesting.
- `RowIndexCounter` is how `RepeatSelection` implements `repeat()`.
- `UnresolvedNullIfMissingField` and `UnresolvedEmptyArrayIfMissingField` are
  *already* missing-field tolerance — precisely the capability a per-dataset
  schema needs.

> **Evidence from the spike.** The prototype branch deleted the `encoders`
> module wholesale and took the toolkit with it. That is why its `query` module
> does not compile: seven files import `ColumnFunctions.structProduct` and the
> old `au.csiro.pathling.view.*` package, and 24 distinct symbols across the
> branch resolve to nothing.
>
> **Splitting this module is worth doing on its own merits**, whether or not
> Parquet on FHIR lands. It reduces the actual encoder replacement to a much
> smaller and more honest diff.

---

## Class-level inventory

Line references are against `main` at the time of survey and will drift.

### Retire — the HAPI bridge (~16 classes)

| Class | Note |
|---|---|
| `EncoderBuilder` | Assembles the `ExpressionEncoder` from pre-built expression trees. |
| `SerializerBuilder`, `DeserializerBuilder`, `Deserializer` | Object → Row and Row → object expression trees. The core of what is being removed. |
| `Catalyst`, `EncodingContext`, `EncoderUtils` | Support scaffolding. `EncoderUtils.defaultResolveAndBind` has no meaning without an encoder. |
| `FhirEncoders`, `FhirEncoderBuilder`, `FhirEncodersKey` | The public handle. `FhirEncoders.contextFor()` is a plain `FhirContext` factory with six callers across `terminology`, `library-api` and the server, and should move rather than die. |
| `CustomCoder`, `DecimalCustomCoder`, `IdCustomCoder` | Per-type serializer overrides. Their *conventions* — `_scale`, `id_versioned` — are separate concerns, listed under Rework. |
| `StaticField`, `GetHapiValue`, `ObjectCast`, `RegisterFid`, `AttachExtensions` | Five of the 16 expressions in `Expressions.scala`. HAPI-object plumbing only. |

### Relocate — query machinery in the wrong module (~20 classes)

| Class | Used by |
|---|---|
| `ColumnFunctions` (`structProduct`, `structProductOuter`) | `ProjectionResult` :104, :146 — view unnesting |
| `ValueFunctions` (`ifArray`, `unnest`, `nullIfMissingField`, `transformTree`, `variantUnwrap`) | FHIRPath engine, 4 import sites |
| `RowIndexCounter` | `RepeatSelection` :65 — the `repeat()` function |
| `SyntheticFieldUtils` | `SingleInstanceEvaluator.sanitiseRow` :384 *(prefix changes — see Rework)* |
| `TraceExpression`, `TraceCollector`, `PruneSyntheticFields` | FHIRPath tracing, 8 import sites |
| `FlexiDecimal`, `FlexiDecimalSupport` | `QuantityMatcher` :276–281, `DecimalCollection` — arbitrary-scale decimal comparison |
| `terminology/ucum/Ucum` | Quantity canonicalisation, 4 sites in `fhirpath` |
| `ResourceTypes`, `ViewDefinitionResource` | Server `ViewResolver`, `SubjectResolver`, `FhirServer` — a HAPI structure definition, not encoder code |
| `FhirConversionSupport`, `R4FhirConversionSupport`, `FhirTraversal` | Bundle entry extraction and `urn:uuid:` reference resolution — operates on HAPI objects, still needed |
| 11 remaining expressions in `Expressions.scala` | See the toolkit list above |

Cost is a module move plus dependency wiring — but only if someone notices they
are there.

### Rework — schema derivation and field conventions

| Class | What changes |
|---|---|
| `SchemaConverter`, `SchemaConverterProcessor` | `resourceSchema(RuntimeResourceDefinition): StructType` is **schema derivation with no Catalyst dependency** — its one tie to the encoder is `buildValue` calling the `CustomCoder` schema hook for the decimal and id conventions. Survives as the comprehensive-schema builder. Drop `createFidField`, `createExtensionField`, `createQuantityFields` and the `CustomCoder` hook; add annotation fields and primitive-extension groups. |
| `EncoderContext`, `EncoderSettings` | The trait carrying `fhirContext`, `dataTypeMappings` and the configuration; `SchemaConverter` extends it. Survives as the schema builder's context, minus `generateFid` and `supportsExtensions`. |
| `SchemaTraversal`, `SchemaProcessor`, `SchemaProcessorWithTypeMappings` | A generic `SchemaVisitor[DT, SF]` over HAPI definitions. Fully reusable — the serializer and schema converter are just two visitors over it. |
| `DataTypeMappings`, `R4DataTypeMappings` | FHIR type → Spark type table. The knowledge survives; the `CustomCoder` hooks do not. |
| `EncodingConfiguration` | `maxNestingLevel` and `openTypes` are meaningless for a focused schema, still required for a comprehensive one — extensions recurse. **Surfaced as `max_nesting_level`, `enable_extensions` and `enabled_open_types` on Python `PathlingContext.create` and R `pathling_connect`**, so their semantics change on a public signature in two languages. |
| `ExtensionSupport` (`_fid`, `_extension`) | Root-level `MAP<INT, Extension>` keyed by per-composite `_fid` becomes inline `extension` groups. Consumers: `Collection.getFid()` :377, `ResourceCollection` :195. **This gets simpler.** |
| `QuantitySupport` (`_value_canonicalized`, `_code_canonicalized`) | Becomes the spec's `__x_canonical` annotation. The spec types it `DECIMAL(38,6)` where `FlexiDecimal` carries arbitrary scale — adopting the annotation verbatim is a capability regression. |
| `CodingSchema` (terminology) | **Highest-risk item.** Fixed 7-field struct decoded **positionally**. See below. |
| `CodingCollection` :140–150, `CodingEquality`, `DecimalCollection` :54, `QuantityEncoding` :112/:205/:297, `FhirFieldNames`, `ExportExecutor` :302 | Consumers binding literal field names or `DecimalCustomCoder.precision()/scale()`. Mechanical, but each needs its own decision about the replacement convention. |

> **Terminology breaks silently on a focused schema.**
> `CodingSchema` declares `{id, system, version, code, display, userSelected, _fid}`
> and reads it by index — `row.getString(CODE_INDEX)` where `CODE_INDEX` is 3.
> Spark's JSON inference over a focused dataset produces a two-field
> `{code, system}` struct. Whatever the field order, every dropped field shifts
> the indices after it: `CODE_INDEX` reads past the end.
>
> Every terminology UDF consumes this struct — `member_of`, `translate`,
> `subsumes`, `display`, `designation`, `property` — and those are **public API
> in Python and R**, called directly on DataFrame columns rather than through
> FHIRPath. `CodingEquality` is better behaved: it compares field-wise by name,
> so it fails loudly at analysis time instead of corrupting.

---

## Feature-level inventory

The encoder is used in five distinct ways. Only one is what the prototype
implements. Mechanisms marked *proposed* are design suggestions, not
observations of existing code.

### A · Text → Row (ingest)

| Site | Disposition | Replacement mechanism |
|---|---|---|
| `PathlingContext.encode` :432 *(public API)* | Rework | `spark.read().json()` + type-fixup transform. **The one the prototype implements, and it works** — at the cost of a schema-inference pass over the data before the read, which an explicit derived schema (decision 4) avoids. |
| `NdjsonSource` :95, :122 | Rework | Same. |
| `PathlingContext.encodeBundle` :540, :560; `BundlesSource` :67 *(public API)* | **Replace** | **No path today.** `spark.read().json()` on a Bundle yields one row with an `entry` array, not N resource rows, and no `urn:uuid:` resolution. *Proposed:* explode `entry.resource` as a transform, resolve URNs in a UDF. |
| XML ingest (`FHIR_XML` mime type) | **Replace** | **No path today.** See the XML note below. |

### B · Row → Text (egress)

| Site | Disposition | Replacement mechanism |
|---|---|---|
| `PathlingContext.decode` :510 *(public API)* | **Replace** | *Proposed:* write-side transform then `to_json()` per row. The prototype has only a file-level `FhirJsonWriter.write(dataset, path)`; this needs the same transforms applied per row. |
| `NdjsonSink` :96; `ExportExecutor` :210–215 | **Replace** | Both route through `decode`. `$export` depends entirely on this. |

### C · Object → Row (no text involved)

| Site | Disposition | Replacement mechanism |
|---|---|---|
| `UpdateExecutor.merge` :157 — *every PUT / POST / $import write* | **Replace** | *Proposed:* HAPI serialises the object to JSON, then read with an **explicit derived schema** — never inferred, or the storage schema varies per request. Depends on the comprehensive schema builder. |
| `ObjectDataSource` :62–63 — *$view / $sql over inline resources* | **Replace** | Same mechanism. This class is duplicated verbatim as test infrastructure in `fhirpath/src/test`. |

### D · Row → Object (no text involved)

| Site | Disposition | Replacement mechanism |
|---|---|---|
| `ReadExecutor` :86–90 — *FHIR read* | **Replace** | *Proposed:* B, then HAPI parses the JSON. Two conversions where there is currently one — a per-row cost on every read and search response. |
| `SearchExecutor` :239, :273; `ViewResolver` :154; `SubjectResolver` :275; `LibraryReferenceResolver` :176, :222 | **Replace** | Same. Five sites, all `filtered.as(encoder).collectAsList()`. |

### E · Schema as contract — no conversion at all

| Site | Disposition | Replacement mechanism |
|---|---|---|
| `QueryHelpers.createEmptyDataset` :50, via `DynamicDeltaSource` :170 and `SnapshotDeltaSource` :92 | Rework | Needs a correctly-typed empty table before any data exists. `SchemaConverter.resourceSchema` answers this directly. |
| `SchemaMigrator` :120 | Rework | Compares the encoder's schema against the Delta table to detect drift. The mechanism survives; its premise — one canonical schema per resource type — holds only for the comprehensive schema. |
| ViewDefinition table schema, via `ViewDefinitionResource` | Rework | A hand-written `@ResourceDef` class **is** the ViewDefinition table's schema. Not in R4, nothing to infer from. The schema builder covers it; the class itself relocates unchanged (see Relocate). |

> **This category is cheaper than it first appears.**
> `SchemaConverter.resourceSchema(RuntimeResourceDefinition): StructType` walks
> HAPI definitions and emits a `StructType`. It is *used by* `DeserializerBuilder`
> and `EncoderBuilder` but does not depend on them, and declares no Catalyst
> expression. Under the no-encoder constraint it survives essentially intact.
>
> That is exactly what the issue describes: *"Comprehensive schema would be
> built using a schema builder based on the HAPI definitions, much like the
> current solution."* The building block already exists and is separable today.

### F · Persisted data written under the current schema

Everything above concerns data in flight. This concerns data at rest, and
neither the prototype nor the issue mentions it.

| Site | Disposition | Replacement mechanism |
|---|---|---|
| Server Delta warehouse — `DynamicDeltaSource`, `SnapshotDeltaSource`, `SchemaMigrator` :120–139 | **Replace** | Tables carry `_fid`, `_extension`, `_scale`, `id_versioned` and the canonicalised quantity fields, and decimals are `DECIMAL(38,6)` where the target is `STRING`. The migrator handles only the additive direction; its own Javadoc states that a table carrying fields the encoders do not emit cannot be migrated and is reported and left alone. Every existing deployment therefore starts in the reported-but-unmigrated state. *Proposed:* a one-off rewrite migration, or a forced re-import behind a version gate. |
| Library `ParquetSink`, `DeltaSink`, `CatalogSink` *(public API, Python + R)* | **Replace** | Users hold Parquet and Delta files in the current layout, written through these sinks. |
| Library `ParquetSource`, `DeltaSource`, `CatalogSource` *(public API, Python + R)* | **Replace** | No source validates the stored schema on read, so an old file flows straight into an engine expecting the new conventions. Needs detection with an actionable error, a read-side compatibility transform, or both. |

> **`site/docs/libraries/io/schema.md` is the published contract for the current
> layout.** It specifies the decimal, ID, quantity, reference and extension
> encodings section by section. Under this change it becomes a rewrite that
> points at the Parquet on FHIR specification, not a touch-up — and it is where
> any compatibility statement for existing files has to live.

---

## Capability register

| Capability | Direction | Detail |
|---|---|---|
| Lossless decimals | **Gain** | Lexical form preserved as `STRING` plus a `__x_numeric` annotation, instead of `DECIMAL(38,6)` + `_scale`. |
| Primitive extensions (`_birthDate { id, extension }`) | **Gain** | Currently unrepresentable — `_fid` is attached per composite and a scalar column has nowhere to hang one. New FHIRPath surface. |
| Smaller schemas | **Gain** | Focused schemas carry only populated elements. |
| Contained resources | Parity | Not in the current encoder schema either. The prototype dropping them is parity, not regression. |
| XML encode / decode *(public API, Python + R)* | **Regression** | Free today because the mime type only selects a HAPI parser. Zero XML references anywhere in the prototype, and the Parquet on FHIR spec never mentions it. |
| Bundle ingest *(public API, Python + R)* | **Regression** | Entry explosion and URN resolution both currently happen on HAPI objects. |
| Canonical quantity comparison | **Regression** | If `__x_canonical` is adopted at its spec'd `DECIMAL(38,6)`, it is narrower than `FlexiDecimal`. `QuantityMatcher` depends on the wider type. |
| Reading Parquet / Delta written by earlier versions *(public API, Python + R)* | **Regression** | Unless category F provides a read-side path, existing files stop being readable by the engine. |

> **XML is harder than a second transformer.**
> Spark 4.0.2 does ship a native XML data source — `XmlFileFormat` is registered
> in `spark-sql_2.13-4.0.2.jar`. But FHIR XML is not an isomorph of FHIR JSON:
> primitives are attributes, `resourceType` is the root element name, primitive
> extensions are children rather than `_`-prefixed siblings, and **a repeating
> element that occurs once infers as non-array**.
>
> That last point matters beyond XML. The prototype reads cardinality from the
> Spark schema, so an element appearing once in one file and twice in another
> produces different engine behaviour. JSON has the same hazard:
> `spark.read().json()` infers `Patient.name` as a struct if every row in a file
> has exactly one name. Normalising inferred cardinality against the definition
> tree may be required for *both* formats.

---

## Test estate

Around 90% of a core library build is test execution, so this is the largest
single line item — and the prototype paid none of it, because it forked the
test suite too.

| Fixture style | Disposition | Reach |
|---|---|---|
| YAML conformance runner (`ArbitraryObjectResolverFactory`) | Relocate | **A JSON path already exists in main:** `spark.read().schema(…).json(…)`, paired with `DefaultDefinitionContext`. Switch the factory. |
| SQL-on-FHIR view tests (`FhirViewTest` :376–383) | Rework | Fixtures are portable JSON, but the pipeline parses to HAPI then encodes. One method changes; the 9 local fixture files (48 cases) and the 22 `sql-on-fhir` submodule files (166 cases) come along free — the submodule needs `git submodule update --init` first. |
| Fluent HAPI builders (`SingleResourceFhirPathTest`, `HapiResolverFactory`, test `ObjectDataSource`) | **Replace** | **39 files** encode HAPI objects directly or via the test `ObjectDataSource`; **67 files** reference `FhirEncoders` at all; 6 helper classes carry the dependency transitively. |

The third row is the expensive one, for three compounding reasons:

1. There is no existing JSON route for these fixtures, unlike the YAML runner.
2. Fixtures deliberately mix populated and empty resources to test empty
   propagation — `new Patient().setId("Patient/3")` exists so an assertion can
   check the null branch. Under focused inference the schema is the union across
   *that test method's own fixture list*, so each test gets its own schema and
   the null branch becomes a missing-field branch. Same answer, different code
   path: the test stops covering what it was written to cover.
3. These tests exercise the encoder as part of the assertion.
   `new Quantity(10.1).setUnit("kg")` produces `_value_scale` and
   `_value_canonicalized`; a quantity comparison test is implicitly a
   `QuantitySupport` test.

> **Coverage risk — reference joins.**
> `reverseResolve` appears in **exactly one test file**:
> `SingleResourceFhirPathTest`, which uses HAPI fixtures. No test resource
> anywhere contains it. `resolve()` is mixed — most of `ResolveFunctionDslTest`
> uses the declarative model builder and already runs through the JSON path, but
> two tests use `withResource(…)` and route to `HapiResolverFactory`.
>
> The join *primitives* — `getResourceKey`, `getReferenceKey` — do have portable
> JSON coverage in `viewTests/ex_fn_rkeys.json` and the `sql-on-fhir` submodule.
> But those only produce keys; they do not perform the join. **The cross-resource
> join machinery, which is the code most sensitive to `id`, `id_versioned` and
> reference column shape, has no portable coverage at all.** Worth closing
> independently of this change.

---

## Decisions this scope depends on

Each changes the size of the work, and none is settled by the prototype.

1. **Does the FHIRPath engine read annotations or recompute them?**
   Annotations are optional in a focused schema, but `DecimalCollection` depends
   on `_scale` and `QuantityEncoding` on `_value_canonicalized` today. Either the
   engine degrades gracefully when `__x_numeric` is absent, or annotations stop
   being optional in practice.

2. **Is inferred cardinality normalised against the definition tree?**
   If not, an element appearing once in one dataset and twice in another produces
   different engine behaviour. If so, the "schema alone is sufficient" premise
   behind schema-driven traversal weakens, and definitions stay in the picture.

3. **Missing field — empty or error?**
   The prototype returns empty for any field absent from the struct, which makes
   `Patient.bogusField` silently empty rather than a modelling error.
   Distinguishing "absent from this dataset" from "not in FHIR" needs a
   definition alongside the schema.

4. **Comprehensive and focused together, or focused only?**
   Category E needs a definition-derived schema. If the comprehensive builder is
   being written anyway, the two are complementary rather than alternative:
   derived schemas where the schema is a contract, inferred schemas where the
   data is bulk.

5. **What happens to XML and bundle ingest?**
   Three options each: convert to JSON with HAPI at ingest, build a second
   transformer, or leave the old path alive. The third splits the codebase
   permanently.

6. **Is the module split done first, as separate work?**
   Separating the query-time toolkit from the HAPI bridge is independently
   valuable and makes the encoder diff legible. Doing it inside the migration
   hides it.

7. **What happens to data already written in the current schema?**
   Category F. Migrate Delta tables in place, force a re-import behind a version
   gate, or carry a read-side compatibility transform for old files. The first
   needs a rewrite the current migrator cannot perform; the second is a breaking
   change for every server deployment; the third keeps both layouts alive in the
   engine. The Python and R libraries have the same exposure through the file
   sinks and sources.

---

*Survey conducted against `main` with the prototype branch checked out at
`.claude/worktrees/spike/parquet-on-fhir` (detached, `8891777ca1`). Line
references reflect the tree at survey time and will drift. Items marked
"proposed" are design suggestions rather than observations of existing code.*
