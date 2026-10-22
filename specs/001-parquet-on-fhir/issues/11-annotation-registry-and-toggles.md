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
  - what to do with any other annotation in the input: *drop* (the default) or *keep*.
  Ticket 18 adds the schema mode. A kind carried in more than one field is governed by one switch.
- **The annotation step, public in `io`**: it takes a dataset of one resource type in this layout and returns it annotated, as the configuration says. `FhirReader.of(spark, definitions)` is unchanged.
- **`encode` runs the step**, and nothing else does. Writing stored data does not add annotations, and neither does reading for a query.
- **Egress leaves out every `__` field**, one Pathling does not define included, rather than throwing.
- **Thread the configuration through `PathlingContext`** in Java, Python and R, and expose the step there.
- **Paths (decision 135).** T053–T055 and T063–T066 name `io/annotation`. The API and the kinds go under `fhir-schema/src/main/java/au/csiro/pathling/schema/annotation/`. T055's test goes in `io`, where it asserts the columns the step emits.
- **Documentation (decision 135, amending 121).** Document the configuration in the Java, Python and R API docs. The site page `site/docs/libraries/io/schema.md` is revised in ticket 15, once there is a kind to write.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [x] The decisions are recorded, and the owner approved them before any kind was written (decision 135).
- [ ] The step runs enabled kinds and skips disabled ones. A test registers a stub kind per switch and shows that disabling one does not affect the others (T055).
- [ ] With stub kinds, a test shows *fill* keeps a present annotation and *recompute* replaces it, and *drop* and *keep* do what they say for a disabled kind and for an unknown `__` field.
- [ ] With a stub kind, a test shows that applying it reads a present annotation and computes an absent one, with the step's Java resolver and with the engine's Catalyst one.
- [ ] The canonical structure places a primitive's annotation entries in its metadata group and a complex element's in its own structure, pinned by a test.
- [ ] `decode` of data carrying an unknown `__` field writes JSON and XML without it, rather than failing.
- [ ] The configuration and the step are reachable from Java, Python and R, with documented defaults.
- [ ] Nothing about stored output changes yet, because no real kinds are registered.
- [ ] If ticket 29 has landed, a dataset ingested with every annotation switch off still carries the layout column.
