# 53 — Drop `id` from System.Coding

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-059, as amended by decision 150. Raised from ticket 51 (Emission shapes, level tests and the by-example tests; owner, 2026-11-12). Example: `design/annotations-by-example.md` §4.4.

**What to build:** Decision 150 and §4.4 make System.Coding `{system, code, version, display, userSelected}`: a System.Coding has no `id`, whether a literal, a terminology result or the Coding a quantity's coded unit is taken as, and a FHIR Coding converted to System loses its `id` and `extension`. The engine still builds the six-field shape with `id`, as `contracts/engine-semantics.md` documents it and `SystemCodingShapeTest` pins it.

The engine's System.Coding is `CodingSchema.DATA_TYPE` in `terminology`. That type is shared: it is also the return type of the library-level terminology functions (`TranslateUdf` and the others) that the library API, Python and R call directly, and the shape `TerminologyHelpers.toCoding` builds. Which of them loses `id` is the scope to decide:

- **(a) Engine only.** Drop `id` from the System.Coding built inside FHIRPath (literals, terminology results, quantity units, and conversion from FHIR), leaving `CodingSchema`, the library-level functions and `toCoding` with `id`. Protected expectation touched: `SystemCodingShapeTest` (six fields to five), already approved for ticket 51 (owner, 2026-11-12). `contracts/engine-semantics.md` changes for the engine's shape.
- **(b) Everywhere.** Drop `id` from `CodingSchema` itself. Protected expectations touched, beyond `SystemCodingShapeTest`: `library-api` `TerminologyHelpersTest.toCoding` (positional field assertions), Python `lib/python/tests/test_udfs.py` (`CodingRow` with `id`, compared with `translate()` results), and R's terminology function tests (`lib/R/tests/testthat/test-udfs-*.R`, to check). `contracts/engine-semantics.md` and `contracts/library-api.md` both document the six-field shape, and the change is a behaviour change of the library API.

Ticket 51 recommended (a), decision 150's scope being the engine's System types, with the library shape following in its own change.

**Not in this ticket:** telling System.Coding apart from FHIR.Coding in the type system (#2804); the equality of Codings (ticket 26).

**Blocked by:** none.

**Status:** needs-triage

- [ ] The scope is decided, (a) or (b).
- [ ] A System.Coding has the shape of decision 150, and `SystemCodingShapeTest` asserts it; under (b), so does the shape the library-level terminology functions and `toCoding` return.
- [ ] `contracts/engine-semantics.md`, and under (b) `contracts/library-api.md`, describe the shape, and each changed protected expectation is approved by the owner.
