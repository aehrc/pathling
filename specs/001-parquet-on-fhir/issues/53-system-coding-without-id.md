# 53 — Drop `id` from System.Coding

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-059, as amended by decision 150. Raised from ticket 51 (Emission shapes, level tests and the by-example tests; owner, 2026-11-12). Example: `design/annotations-by-example.md` §4.4.

**Triage (owner, 2026-11-15).** Deferred to stage 5. It is not part of PR #2877 (stage 4), and it stays before 11.0.0, so that the Coding shape changes only once. The scope is wider than the title, as below, and the ticket is rewritten to match.

**What to build.** System.Coding is the shape the terminology service returns. It is one type, used everywhere in the engine and at the library level. FHIR.Coding is used only where data is exchanged directly with FHIR. So the change applies everywhere, not to the engine alone (option (b) of the earlier draft; option (a), engine only, is not taken):

- **The System.Coding has the fields the terminology service can return, and nothing else.** It was shaped like FHIR.Coding because the two types were not told apart. The likely fields are `system`, `code`, `display` and perhaps `version`. `id` goes. `userSelected` is a candidate for removal too, and the first of the candidates. Whether `version` stays is open: check which fields the terminology service's results actually populate.
- **One shape, wherever a System.Coding is built.** `CodingSchema` (in `terminology`), and so the engine, the library-level terminology functions (`TranslateUdf` and the others) and `TerminologyHelpers.toCoding`, all take the new shape.
- **The Coding literal moves with it.** Its syntax, `<system>|<code>[|<version>][|<display>[|<userSelected>]]`, is reconsidered with the shape, and with it `CodingLiteral`, `CodingCollection.buildColumn`, `CodingDecoder` and `ImmutableCoding`. Removing a component from the literal is a user-visible FHIRPath change.
- **A stored FHIR Coding keeps `id` and `extension`,** and is converted to System.Coding where it meets terminology or a System value.

**Known so far (read-only, on `issue/2832`, not a complete audit).** Apart from the literal, the schema and the decoder, no code sets or reads `userSelected`, so it reaches a System.Coding only through the literal's fifth component.

**Relations to make explicit when this is picked up.**

- #2804, telling System types apart from FHIR types in the type system. This ticket does not do that, but the conversion from FHIR.Coding to System.Coding is the same boundary.
- Decision 79's addendum ("a stored Coding combined with a System Coding keeps its extensions"). A stored Coding is reconciled with a System one by name, not converted, so its `extension` survives a union. Converting to System.Coding where they meet changes that, and the addendum has to be revisited with it.

**Protected expectations, each to be approved by the owner when the ticket is picked up:** `SystemCodingShapeTest`; `library-api` `TerminologyHelpersTest.toCoding` (positional field assertions); Python `lib/python/tests/test_udfs.py` (`CodingRow`, compared with `translate()` results); R's `lib/R/tests/testthat/test-udfs-*.R` (not yet checked); and any YAML conformance case that uses the five-part literal (not yet checked).

**Contracts to update:** `contracts/engine-semantics.md` and `contracts/library-api.md`, which both document six fields. The change is a behaviour change of the library API.

**Not in this ticket:** the type-system split itself (ticket 56, #2804), which this ticket now depends on; the equality of Codings (ticket 26).

**Blocked by:** 56 (Tell System types from FHIR types in the engine, for Coding and Quantity), which tracks #2804. The separation goes first, so that the Coding shape changes only once (owner, 2026-11-16).

**Status:** needs-triage (PoF_line issue; the scope above is the owner's, the field list is not yet settled)

- [ ] The fields of System.Coding are settled, `version` and `userSelected` included, from what the terminology service returns.
- [ ] `CodingSchema`, the engine, the library-level terminology functions and `toCoding` build that shape, and `SystemCodingShapeTest` and `TerminologyHelpersTest.toCoding` assert it.
- [ ] The Coding literal's syntax and its builders match the shape.
- [ ] A stored FHIR Coding is converted to System.Coding where it meets terminology or a System value, and decision 79's addendum is amended to match.
- [ ] `contracts/engine-semantics.md` and `contracts/library-api.md` describe the shape, and each changed protected expectation is approved by the owner.
