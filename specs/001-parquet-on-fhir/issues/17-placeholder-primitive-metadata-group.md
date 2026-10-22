# 17 — PLACEHOLDER: the primitive metadata group (US6, #437)

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-003, FR-016, FR-019, FR-034, US6. Old tasks: T051, T061, T061a, T078c, T120–T124.

**What to build:** Primitive element ids and extensions are stored in the metadata group beside the element, round-trip losslessly, and are navigable from FHIRPath. It is built on the interim ingest of decision 134: Spark's reader and the generated transform for JSON, and the `from_xml` route of ticket 32 for XML and bundles. To be rewritten once ticket 32 lands. If ticket 16 later replaces the ingest, it carries the group over.

Points carried from the frozen tasks:

- **Remove the strip.** The strip that keeps the group out of a fitted schema comes out (decision 60).
- **Positional nulls.** Egress stops dropping an array's nulls one at a time, because the alignment is positional (decision 63). XML egress must keep positions too.
- **`_x` without `x`.** A repeating primitive carried only as `_x` needs a value array of positional nulls, not a null array (decision 72).
- **Group content.** The content of a group is checked. Decide whether `_id` and `_url` groups exist at all.
- **Resources without a domain-resource root.** A group's extensions take the Extension type's own shape, including on `Binary`, `Bundle` and `Parameters`, which have no root `extension` (T061a).
- **Remove the exclusion (T078c).** The primitive-metadata exclusion leaves the round-trip harness and the ingest-format round-trip test, and the carve-out leaves the spec. State what the dense mode does with the group, rather than deleting that sentence.
- **Navigation.** Navigation to the id and extensions goes through sibling resolution. Both yield empty where the source carried neither (T120–T124).

**Blocked by:** 32, and 11 for the engine's pairing of `x` with `_x`

**Status:** needs-triage

- [ ] To be written once ticket 32 lands.
- [ ] Once primitive metadata is stored, a `urn:` reference inside a primitive's extension resolves as decision 83 states, in XML and JSON bundles alike (owner, 2026-10-22; ticket 32 accepted it as a gap until then).
