# 40 — Specify the shape of SQL output

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-021, FR-059; `contracts/engine-semantics.md`; decision 135. Raised from ticket 12's first review item (owner, 2026-10-26).

**What to decide:** the shape of what the engine hands back as SQL, where a result leaves FHIRPath for Spark columns: `fhirPathToColumn`, and view columns where they allow a complex type.

Today the shape follows whatever the plan built. Ticket 12 kept the canonical annotations on a returned quantity for now (owner, 2026-10-26), as 9.9.0 kept `_fid`, `value_scale` and the canonical companions there. `evaluateFhirPath` strips annotations, but only when it renders JSON. Nothing specifies either behaviour.

The specification should cover at least:

- **Pruning annotations.** Whether `__` fields leave the engine, and if they do, whether the FR-021 switches govern them.
- **Fixed types for primitives.** One Spark type per FHIR primitive in output, whatever the stored or computed representation. For example, a decimal is returned as one type, not as the value of one plan and the FlexiDecimal of another. Ticket 41 makes that type `DECIMAL(38,6)`, and gives the System.Quantity `_value.__numeric`, which the output shape records.
- **A unified, complete schema for complex types.** A complex result could have one schema derived from the definitions, rather than the fitted shape of the dataset it was read from. Then the same expression returns the same type over any dataset.

Out of scope: JSON and XML egress, whose shape is FHIR's.

**Blocked by:** None. It is best taken once tickets 12–14 have settled which annotations exist.

**Status:** needs-triage

- [ ] The owner agrees the output shape for annotations, primitives and complex types, and it is recorded in the specification.
- [ ] Tickets are raised for whatever the agreed shape requires the engine to change, or for the follow-up issues through ticket 26.
