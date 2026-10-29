# 36 — Scope the follow-up issue on extra fields: their support, and their place in the conformance levels

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-008, FR-012, FR-016, FR-063. "Conformance levels", "The round-trip invariant", "Extra elements" and "Typing undescribed content in XML" in `evidence/ingress-strictness/README.md`; decisions 133, 135 and 136; [ADR-0005](../../../docs/adr/0005-types-from-definitions-only.md). Replaces the scope of ticket 34.

**What to build:** The written scope of one follow-up GitHub issue, which ticket 26 raises. #2367 does not build any of it (owner, 2026-10-22). The issue covers support for extra fields: content and layout that the definitions do not derive. It also defines where each kind of extra field stands in the conformance levels. Today the levels say only that a `__` field is layout (levels 2 and 3, decision 135), and that content the definitions do not describe is outside the round-trip invariant.

**The forms an extra field takes.**

- **A `__` field Pathling does not define**, such as another producer's annotation. A Parquet dataset may carry any, and the annotation step can keep them (decision 135). Its levels are settled. Its handling beyond that is open: whether the engine reads it, and whether a kept one is checked against its element.
- **A column the definitions do not describe**, reaching a stored table through ingest or without it: fields from another FHIR version, vendor or pipeline keys, `fhir_comments`, a merge that appended a field, another producer's `contained`, and a legacy `_fid` or `_extension` column (tickets 20 and 34).
- **A key or element in JSON or XML input** that the definitions do not describe, including one named like a layout field.

**What the issue has to settle, for each form.**

- **Its level**, for JSON and for Parquet. Level 2 applies to such content, since it needs no definitions. Level 3 is where "the definitions do not describe it" is decided, so the issue states whether an extra field fails level 3 or is allowed by it, as `__` fields now are.
- **What `trust` promises about it**, and what the other modes do, in step with the follow-up issue on non-conformant data that ticket 26 also raises.
- **What ingest, the engine and egress do with it.** Options recorded so far:
  - D1, user-supplied definitions, and D2, types inferred from the data, under "Extra elements", with D2's costs and the proposal to infer FHIRPath System types;
  - a JSON-type marker in the Parquet metadata, and Parquet's `JSON` logical type, for a number stored as text, whose support in Spark 4 and survival through Delta and pyarrow are unmeasured;
  - for XML, which carries no types: namespaced attributes, and a comment carrying the undescribed element, under "Typing undescribed content in XML";
  - that JSON and XML give such content different types, against principle 6;
  - the parked branch `issue/2830-egress-unknown-field` (`bead739730`) from ticket 20;
  - whether a column expression stays unbound to a schema (decision 136, owner, 2026-10-23). Supporting such content may need a schema the definitions do not give, and an implementation that does may require binding the expression to the dataset or to that schema.
- **Whether the round-trip invariant can extend to it**, even only in datasets Pathling wrote, through metadata that preserves its representation.

**Blocked by:** None. Ticket 26 raises the issue once this scope is agreed.

**Status:** ready-for-human

- [ ] The issue's scope is written up from this ticket, and the owner agrees it.
- [ ] Ticket 26 raises it, and records its number.
