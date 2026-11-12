# 17 — Primitive metadata in the stored layout (US6, #437)

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-003, FR-016, FR-019, US6. Old tasks: T051, T061, T061a, T078c, T078e. Decisions 60, 63, 72, 83, 134, 135 and 141. Split in two (owner, 2026-11-12): this ticket stores the metadata group and round-trips it, and ticket 52 (Primitive metadata in the engine) reads it.

**What to build:** Primitive element ids and extensions are stored in the metadata group beside the element and round-trip losslessly, in JSON, in XML and inside bundles, when an ingest option asks for them. It is built on the interim ingest of decision 134: Spark's reader and the generated transform for JSON, and the `from_xml` route of ticket 32 (XML and bundles without HAPI) for XML and bundles. If ticket 16 (the ingest and egress rework) later replaces the ingest, it carries the group over.

**The option (owner, 2026-11-12).** Storing a primitive's ids and extensions is an ingest option, off by default. A dense schema is already wide, and the group adds to it, so it is paid for only where it is needed. With the option off, ids and extensions are dropped as today, silently. The engine's own switch for reading them is a separate setting (ticket 52): the engine reads data stored with the group without paying for it in every query. Which further options limit the full dense schema is decided in ticket 18 (Dense mode), together with an impact analysis of the width and depth of the full schema on storage and performance.

Points carried from the frozen tasks:

- **Remove the strip.** The strip that keeps the group out of a fitted schema comes out (decision 60).
- **Positional nulls.** Egress stops dropping an array's nulls one at a time, because the alignment is positional (decision 63). XML egress must keep positions too.
- **`_x` without `x`.** A repeating primitive carried only as `_x` needs a value array of positional nulls, not a null array (decision 72).
- **Group content.** The content of a group is checked. The `_id` and `_url` groups stay as the canonical structure emits them, because annotations use them (owner, 2026-11-12). Whether groups FHIR does not define are compliant is a question for the non-conformant-data follow-up of ticket 26 (Raise the GitHub follow-up issues).
- **Resources without a domain-resource root.** A group's extensions take the Extension type's own shape, including on `Binary`, `Bundle` and `Parameters`, which have no root `extension` (T061a).
- **Remove the exclusion (T078c).** The primitive-metadata exclusion leaves the round-trip harness and the ingest-format round-trip test as an unconditional rule. With the option on, the round trip is lossless; with it off, FR-016's exception 3 holds and the harness asserts it. The carve-out is reworded in the spec, not deleted. State what the dense mode does with the group: a dense schema carries it only when the option is on, and then beside every primitive, populated where the source has ids or extensions and null otherwise (owner, 2026-11-12).
- **The group is the annotations' container.** A primitive's annotations are fields of its metadata group, after `id` and `extension` (decision 135, part 4), so the group can exist without ids or extensions. The reader emits a group only once this ticket populates it, and ticket 11's annotation step adds it where the reader did not (decision 60's addendum).

**A `urn:` reference inside a primitive's extension (decision 83; owner, 2026-10-22, confirmed 2026-11-12).** Once the group is stored, a Reference inside it resolves as decision 83 states, in XML and JSON bundles alike. It is found by type, not by the JSON key `reference`, which other R4 elements share. Ticket 32 accepted this as a gap until now.

**Upstream amendment.** Row 12 of `evidence/upstream-amendments-draft.md` (repeating primitives, positional nulls, `_x` without `x`, and the specification's own example) is drafted from the result. Ticket 46 (Review the proposed amendments to the Parquet on FHIR specification) waits on it for that row.

**Blocked by:** Nothing now: ticket 32 has landed. Ticket 52 follows this ticket.

**Status:** ready-for-agent

- [ ] With the option on, a primitive's id and extensions are stored in the group beside it and round-trip losslessly, from JSON, XML and bundles, for singular and repeating primitives, including positional nulls and `_x` without `x` (T051).
- [ ] With the option off, nothing changes in the stored schema, and FR-016's exception 3 holds and is asserted by the harness.
- [ ] A group's content is checked, and its extensions have the Extension type's shape on `Binary`, `Bundle` and `Parameters` (T061a).
- [ ] The strip is gone, the exclusion is conditional on the option, and the spec says what the dense mode does with the group (T078c).
- [ ] A `urn:` reference inside a primitive's extension resolves as decision 83 states, in XML and JSON bundles; a test shows each.
- [ ] Row 12 of the upstream amendments draft is drafted, and the `_id` and `_url` question is recorded in ticket 26's follow-up.
