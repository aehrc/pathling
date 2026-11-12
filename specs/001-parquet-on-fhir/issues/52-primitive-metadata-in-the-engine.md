# 52 — Primitive metadata in the engine (US6, #437)

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-034, US6. Old tasks: T120–T124. Decisions 135 (part 4), 141, 142 and 152. Split from ticket 17 (Primitive metadata in the stored layout; owner, 2026-11-12).

**What to build:** The engine pairs a primitive's values with its metadata group by position and reads its ids and extensions, behind a switch.

- **Pairing.** Ticket 11 (Annotation registry, per-annotation toggles and SchemaConfiguration) builds the pairing of `x` with `_x` for annotation fields, and ticket 51 (Emission shapes, level tests and the by-example tests) builds its emission shape for repeating primitives. This ticket extends it to `id` and `extension`, for singular and repeating elements alike. It names only the fields an expression needs, so that the read prunes. Nothing tests whether `_x` exists on its own.
- **Navigation.** Navigation to the id and extensions goes through sibling resolution (T124), and both yield empty where the source carried neither (T120–T123).
- **The switch (decision 141, D14; owner, 2026-11-10).** A configuration option, off by default, puts a primitive's `id` and `extension` into its engine element as content fields beside `value` (decision 152). With it off, `x.id` and `x.extension(…)` on a primitive are empty, with no error. It is a separate setting from the ingest option of ticket 17 (owner, 2026-11-12), so that data stored with the group can be read without it.
- **Conversion to System and a System merge (decision 142, D8 and D9; moved from ticket 43, owner, 2026-11-11).** With the switch on, the conversion to System drops `id`, `extension` and any other content the System type lacks, and a merge whose result is a System value drops each operand's `id` and `extension`, while a merge of FHIR values unions them. The rules are as decision 153 now states them, which changes what a converted System value is made from. They are not restated here.
- **Equality.** Primitive `id` and `extension` in comparisons stay as decision 151 settles, until the equality review of ticket 26.

**Blocked by:** 17, for the stored group; 51, for the pairing's emission shape; and 50 (Decimals and System.Quantity in the engine), for decision 153's conversion rules.

**Status:** ready-for-agent

- [ ] With the switch on, `x.id` and `x.extension(…)` return the stored values for a singular and a repeating primitive, and empty where the source carried neither; with it off they are empty with no error. A test shows each.
- [ ] The read names only the fields the expression needs, shown by a plan test.
- [ ] With the switch on, the conversion to System drops `id`, `extension` and any other content the System type lacks, a merge whose result is a System value drops `id` and `extension`, and a merge of FHIR values unions them; a test shows each, over a primitive that carries an extension.
