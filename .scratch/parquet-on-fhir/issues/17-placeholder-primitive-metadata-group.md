# 17 — PLACEHOLDER: the primitive metadata group (US6, #437)

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-003, FR-016, FR-019, FR-034, US6. Old tasks: T051, T061, T061a, T078c, T120–T124.

**What to build:** Primitive element ids and extensions are stored in the metadata group beside the element, round-trip losslessly, and are navigable from FHIRPath. To be rewritten once ticket 16 lands, because its shape depends on the ingest mechanism.

Points carried from the frozen tasks:

- **Remove the strip.** The strip that keeps the group out of a fitted schema comes out (decision 60).
- **Positional nulls.** Egress stops dropping an array's nulls one at a time, because the alignment is positional (decision 63). XML egress must keep positions too.
- **`_x` without `x`.** A repeating primitive carried only as `_x` needs a value array of positional nulls, not a null array (decision 72).
- **Group content.** The content of a group is checked. Decide whether `_id` and `_url` groups exist at all.
- **Resources without a domain-resource root.** A group's extensions take the Extension type's own shape, including on `Binary`, `Bundle` and `Parameters`, which have no root `extension` (T061a).
- **Remove the exclusion (T078c).** The primitive-metadata exclusion leaves the round-trip harness and the ingest-format round-trip test, and the carve-out leaves the spec. State what the dense mode does with the group, rather than deleting that sentence.
- **Navigation.** Navigation to the id and extensions goes through sibling resolution. Both yield empty where the source carried neither (T120–T124).

**Blocked by:** 16

**Status:** needs-triage

- [ ] To be written once ticket 16 lands.
