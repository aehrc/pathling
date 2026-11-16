# 56 — Tell System types from FHIR types in the engine, for Coding and Quantity

> **Tracked as #2804** (Distinguish System.Coding from FHIR.Coding (and System.Quantity from FHIR.Quantity) in the FHIRPath engine). The issue's body is this ticket's scope.

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-059, and the out-of-scope line that names #2804. Contract: `contracts/engine-semantics.md`, the System Coding section. Decision 79's addendum.

**Triage (owner, 2026-11-16).** This ticket and ticket 53 (Drop `id` from System.Coding) were stage 5 tickets, and now become GitHub issues: this one as #2804, and 53 as its own `PoF_line` issue. The type separation goes first: ticket 53's reshaping depends on it, so that the Coding shape changes only once. The specification still lists #2804 as outside it; the owner's decision brings it in. `spec.md` (around lines 527 and 666) and `contracts/engine-semantics.md` (the System Coding section) currently say #2804 is outside the specification, and work on this ticket must update them.

**What to build.** The engine tells `System.Coding` from `FHIR.Coding`, and `System.Quantity` from `FHIR.Quantity`. Today the same types are set for literals, engine-built values and stored elements alike: `CodingCollection.build` sets both `FhirPathType.CODING` and `FHIRDefinedType.CODING`, and `QuantityCollection.build` does the same for Quantity. So a Coding literal satisfies `is(FHIR.Coding)` and `is(System.Coding)` equally, and so does a stored Coding element. The same holds for Quantity.

The model, as in the FHIR binding, where a FHIR type converts implicitly to its System counterpart:

- **Terminology functions** (`memberOf`, `translate`, `subsumes`, `display`, `property`, `designation`, and the matching UDFs) take and return `System.Coding`, whose schema is fixed and independent of how a `FHIR.Coding` is stored.
- **Stored Codings and Quantities** are FHIR types, whose stored shape varies with the layout. They convert implicitly to their System counterparts where a System value is expected.
- **Literals and engine-built values** are System types, and carry no `id` or `extension`.

It affects `is()`, `ofType()`, `toString()` (#2802), the conversion functions (#2803), and whether extensions survive combining a stored value with a literal (decision 79's addendum). It is a change to the type system, which every later feature inherits, so it needs a design before it is built.

**Not in this ticket:** the fields of `System.Coding` and the Coding literal (ticket 53).

**Blocked by:** None.

**Status:** needs-triage (PoF_line issue; the scope is #2804's body, and the design is not yet written)

- [ ] A design for the separation is written and approved by the owner before implementation.
- [ ] Literals and engine-built values are `System.Coding` and `System.Quantity`, and stored elements are `FHIR.Coding` and `FHIR.Quantity`, for `is()`, `ofType()` and the conversion functions.
- [ ] A stored Coding or Quantity converts to its System counterpart where a System value is expected, terminology functions first, and decision 79's addendum is revisited with it.
- [ ] `contracts/engine-semantics.md` and the specification's out-of-scope line describe the separation, and each changed protected expectation is approved by the owner.
