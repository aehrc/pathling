# 11 — Annotation registry, per-annotation toggles and SchemaConfiguration

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-021, FR-022; ADR-0003. Old tasks: T063, T055; decisions 69, 84, 121, 135.

**What to build:** The framework that defines annotations once for the engine and for ingest, and the step that stores them, with each kind on by default and individually disableable. It defines no annotation kind of its own. Those land in tickets 12–14.

Decision 135 settled the questions this ticket raised: where the definitions live, how wide a dense schema's annotations are, and how the toggles reach the data. It replaces the earlier plan of package-private processors in `io` driven from the reader.

- **The annotation API in `fhir-schema`**, in `au.csiro.pathling.schema.annotation`. It is public and abstract: a kind is a function from the stored value of the element it annotates to the annotation's value. The registry maps a canonical-structure annotation entry to its kind, and takes order from `LayoutFields` rather than declaring its own. It applies a kind for both clients: the engine reads the stored annotation where the schema carries it and computes it otherwise, decided once per schema. Applying a kind takes a resolver, which resolves a field to the stored one where present and to a fallback otherwise: a Java one for the step, which knows the schema, and a Catalyst one for the engine beside `ResolveOrNull`, which `fhir-schema` may not hold. The concrete kinds go in a separate package beside the API.
- **Move the annotation entries to their container (decision 135, part 4).** `DefinitionCanonicalStructure` and `LayoutFields` today place annotations as `__<element>_<annotation>` siblings. Move a primitive's into its metadata group's structure, after `id` and `extension`, and a complex element's to the end of its own structure, named `__<annotation>`. No kind is registered, so stored output does not change.
- **The set of kinds is closed, and tests still supply stubs.** The public entry points, `PathlingContext` and the annotation step's public factory, take a `SchemaConfiguration` alone and always use the built-in kinds, of which this ticket has none. A registry can be built from a list of kinds, but only through a factory the public entry points never expose: package-private in `io` for the step, so that `io`'s tests reach it. Every kind, a stub included, names the switch that governs it: numeric, range or canonical.
- **`SchemaConfiguration` in `fhir-schema`**, beside `EncodingConfiguration` and not on it (FR-051). It carries:
  - one switch per kind, all on;
  - what to do with an annotation already present: *fill* (the default) or *recompute*;
  - what to do with an annotation of a kind that is off: *drop* (the default) or *keep*;
  - what to do with a `__` field no kind computes there: *keep* (the default) or *drop* (decision 135's addendum of 2026-10-23).
  Ticket 18 adds the schema mode. A kind carried in more than one field is governed by one switch.
- **The annotation step, public in `io`**: it takes a dataset of one resource type in this layout and returns it annotated, as the configuration says. `FhirReader.of(spark, definitions)` is unchanged.
- **`encode` runs the step**, and nothing else does. Writing stored data does not add annotations, and neither does reading for a query.
- **The engine pairs `x` with `_x` (decision 135, part 4)** by position, for the annotation fields only. Ticket 17 extends the pairing to ids and extensions.
- **The step adds a metadata group the reader did not emit**, where a primitive's annotation needs one, as it must for stored data written without it.
- **Egress leaves out every `__` field**, one Pathling does not define included, rather than throwing.
- **Thread the configuration through `PathlingContext`** in Java, Python and R, and expose the step there.
- **Paths (decision 135).** T053–T055 and T063–T066 name `io/annotation`. The API and the kinds go under `fhir-schema/src/main/java/au/csiro/pathling/schema/annotation/`. T055's test goes in `io`, where it asserts the columns the step emits.
- **Documentation (decision 135, amending 121).** Document the configuration in the Java, Python and R API docs. The site page `site/docs/libraries/io/schema.md` is revised in ticket 15, once there is a kind to write.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [x] The decisions are recorded, and the owner approved them before any kind was written (decision 135).
- [x] The step runs enabled kinds and skips disabled ones. A test registers a stub kind per switch and shows that disabling one does not affect the others (T055).
- [x] With stub kinds, a test shows *fill* keeps a present annotation and *recompute* replaces it, and *drop* and *keep* do what they say for a disabled kind and for an unknown `__` field.
- [x] With a stub kind, a test shows that where the schema carries `_x` but not `x`, *fill* keeps a stored annotation and *recompute* computes it as for a null value, and that neither adds one.
- [x] With a stub kind, a test shows that applying it reads a present annotation and computes an absent one, with the step's Java resolver and with the engine's Catalyst one.
- [x] With a stub kind on `birthDate`, a test shows the step stores `_birthDate` with the annotation for a source with no `_birthDate`, and for stored Parquet without one.
- [x] The canonical structure places a primitive's annotation entries in its metadata group and a complex element's in its own structure, pinned by a test.
- [x] With a stub kind on a repeating primitive, a test shows that the engine pairs each value with its own stored annotation by position, including where a value in the middle has none, rather than reading the annotations through the traversal, whose null removal loses the positions.
- [x] `decode` of data carrying an unknown `__` field writes JSON and XML without it, rather than failing.
- [x] The configuration and the step are reachable from Java, Python and R, with documented defaults.
- [x] Nothing about stored output changes yet, because no real kinds are registered.
- [ ] If ticket 29 has landed, a dataset ingested with every annotation switch off still carries the layout column.

## Comments

### Implementation (branch issue/2832_11_annotation-registry)

**What was built, and where.**

- **Placement (decision 135, part 4).** `LayoutFields` names the annotations `__numeric`, `__start`, `__end`, `__canonical` and `__canonical_exact`, and `annotationNames` takes the type alone. `DefinitionCanonicalStructure` no longer places annotations beside an element: a complex element's close its own structure, and a primitive's are the last fields of its `MetadataGroupStructure`, which now knows its primitive and exposes `annotationEntries()`. Pinned in `DefinitionCanonicalStructureTest`; `AnnotationFreeSuiteTest` (fhirpath) follows the new paths.
- **The API, `fhir-schema/.../schema/annotation/`.** `AnnotationKind` (abstract: switch, field names, `compute(value)` returning a structure of the kind's fields, and `apply(field, value, container, repeating, resolver)`), `AnnotationResolver` (stored field where the schema carries it, fallback otherwise), `AnnotationRegistry` (maps an annotation entry to its kind by name; order stays with `LayoutFields`; `builtIn()` is empty; `of(kinds)` is how tests supply stubs), and `AnnotationSwitch` (numeric, range, canonical; a final class with constants rather than an enum, per CONTRIBUTING). No kinds package yet, since there is no kind to put in it.
- **`SchemaConfiguration`** in `fhir-schema`'s `au.csiro.pathling.schema`, where T033c deleted it from and T063 says to recreate it: `enableNumericAnnotation`, `enableRangeAnnotation`, `enableCanonicalAnnotation` (all true), `recomputeAnnotations` (false, so *fill*), `keepOtherAnnotations` (false, so *drop*). Booleans rather than two-valued types, as the deleted class reasoned.
- **The Catalyst resolver** is `ResolveOrElse` in `encoders/.../sql/`, beside `ResolveOrNull`, exposed as `ColumnFunctions.resolveOrElse`.
- **The engine's pairing** is `DefaultRepresentation.traverseAnnotated`: per parent, it reads `x` and `_x`, applies the kind with the Catalyst resolver to the groups as an array (so the read names only the annotation field), zips by position before any null is removed, then flattens and drops positions without a value. Nothing in the engine calls it yet; tickets 12 to 14 do. Tested in `AnnotatedTraversalTest`.
- **The step** is `io`'s public `Annotator`: `of(definitions, configuration)` uses the built-in kinds, and the package-private `of(definitions, configuration, kinds)` takes stubs. It walks the input schema, rebuilds only the structures where something changes, places derived annotations at the end of their container in canonical order with kept ones after them, adds a metadata group after its primitive where the input has none, and aligns a repeating primitive's groups with its values by position. With the built-in kinds it returns the same dataset. Tested in `AnnotatorTest`.
- **Egress** leaves out every `__` field, at any depth (`ResourceTransformer.egressFields`). Tested for JSON and XML in `EgressAnnotationTest`, through `FhirWriter`, which is what `PathlingContext.decode` calls.
- **`PathlingContext`** takes `schemaConfiguration(...)` on its builder, exposes `getSchemaConfiguration()` and `annotate(resources, resourceName)`, and runs the step in `encode` and `encodeBundle` only. Python: five keyword arguments on `create`, appended after the existing ones, and `annotate`. R: five arguments on `pathling_connect` and `pathling_annotate`. Defaults are documented in the Javadoc, docstrings and roxygen.

**Deviations, and why.**

- *Fill* is decided per schema, not per row: where the schema carries an annotation it is kept as stored, including a row whose stored value is null. This keeps the step's resolver the same contract as the engine's. See the first review item.
- `site/docs/libraries/io/schema.md` is untouched, as the ticket says (ticket 15).
- The ticket 29 criterion does not apply: ticket 29 has not landed on this branch, so there is no layout column to carry.

**Test runs.**

- `mvn clean install -pl library-runtime -am`, all green: utilities 35, fhir-schema 43, encoders 601, io 495 (1 skipped, as before), terminology 538, fhirpath 7397 (922 skipped, the exclusion baseline), library-api 328, with 0 failures and 0 errors.
- Python: `pytest tests` gave 571 passed. It was run directly with a private Ivy home, and the retrieved `library-runtime` jar's SHA-1 matched `library-runtime/target`.
- R: `devtools::test()` gave 220 passed, 0 failed. It ran with a private `SPARK_CONF_DIR` setting `spark.jars.ivy`, and the jar's SHA-1 matched. styler and lintr are clean on the changed files.

**For the owner's review.**

1. **Should *fill* be per row?** "Computing only the missing ones" could mean `coalesce(stored, computed)` per row, which would repair merged datasets whose rows from unannotated files carry a null. It is per schema today. My recommendation is per row in the step only (the engine stays per schema, FR-023). It is a small change in `Annotator.resolver`, but it changes what *fill* means, so I left it for you.
   **Owner (2026-10-22):** keep per schema for now; the per-row option is considered in stage 5, as ticket 38.
2. **Recompute where a group has no value beside it.** Under *recompute*, a stored annotation in a group whose primitive is absent from the schema cannot be recomputed. It falls to the *drop* or *keep* rule. This is undefined under `trust`, so I left it.
   **Owner (2026-10-23):** the primitive is taken as null in every row; see decision 135's addendum of 2026-10-23, which also splits the other annotations into unrequested and unknown ones, and keeps unknown ones by default.
3. **The pairing's result shape.** `traverseAnnotated` returns items of `struct(value, <annotation field>)` for one field of one kind. Tickets 12 to 14 may want several fields, or the collection's own representation. I kept it to what this ticket's test needs.
4. **The review flagged `Optional` parameters** in `Annotator` (Sonar S3553). The surrounding `io` code does not use them, but restructuring the step's private methods around them would cost readability for no behaviour, so I left them for you to judge.


### Review fixes (2026-10-23)

The owner's decisions on the code review are decision 135's addendum of 2026-10-23. What changed from the implementation above:

- **The switches.** `keepOtherAnnotations` is replaced by `keepUnrequestedAnnotations` (a kind that is off; dropped by default) and `dropUnknownAnnotations` (a `__` field no kind computes where it is found; kept by default), in Java, Python and R, with the guarantees of each setting in their docs. An annotation is unknown by where it is found: a known kind's name in a container whose type does not carry that kind is unknown there. With the built-in kinds every `__` field is unknown, so the default `annotate` now returns its input unchanged.
- **A missing `x`.** Where the schema carries `_x` but not `x`, a stored annotation of a kind that is on is kept by *fill* and computed for a typed null (decision 72) by *recompute*, and none is added.
- **`AnnotationKind.apply` takes one value** and no `repeating` flag, and pairing a repeating primitive is linear rather than quadratic in its length. The step applies the kind at each position, to the value and the group there. The engine resolves the annotation field over all the groups at once and zips it with the values, because reading it per position would read each group whole rather than prune to the field; `AnnotatedTraversalTest` pins the pruned read. `compute(value)` stays a function of the value alone; a kind derived from extensions would also take the metadata group, and for a repeating `x` take positions from it.
- **Pass-through.** The step counts a container as changed only where an annotation is computed, left out or moved, so data it has annotated is returned unchanged by *fill*.
- **A metadata group of the wrong shape** (not a structure, or not an array of them beside a repeating primitive) is passed through as it is, rather than failing the step.
- **`traverseAnnotated`** takes the field's FHIR type and decodes each paired value as `traverse` does; the annotation is still computed from the stored text.
- **Shared rules.** `ResolveOrNull` resolves through `ResolveOrElse` with a typed null; `LayoutFields.isAnnotation` and `io`'s `ObservedFields` replace the step's and egress's own copies. `PruneSyntheticFields` keeps the previous encoder's own rule, which differs.
- **`evidence/scripts/DenseAnnotationWidth.java`** is pinned to `782941609a`, where it still compiles; ticket 18 replaces it.

**Test runs after the review fixes.**

- `mvn clean install -pl library-runtime -am`, all green: utilities 35, fhir-schema 43, encoders 601, io 499 (1 skipped), terminology 538, fhirpath 7398 (922 skipped, the exclusion baseline), library-api 328, with 0 failures and 0 errors. The last commit, which changes only the engine's pairing, was then rerun through `AnnotatedTraversalTest`.
- `mvn install -pl lib/python,lib/R` with a fresh private Ivy home through `SPARK_CONF_DIR`, the retrieved `library-runtime` jar's SHA-1 matching `library-runtime/target`: Python 571 passed, R 222 passed, 0 failed.
