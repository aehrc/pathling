# 34 — PLACEHOLDER: Carry content the definitions do not describe through ingest, egress and the engine

**Spec:** None yet. This needs a specification of its own. Today's behaviour is set by `specs/001-parquet-on-fhir/spec.md`: US1 acceptance scenario 5, FR-008, FR-012, FR-016 item 3, FR-025 and FR-063, and [ADR-0005](../../../docs/adr/0005-types-from-definitions-only.md). Old task: T124c, by way of ticket 20.

**What to build:** Content the definition set does not describe is supported across every component, rather than dropped. The owner's direction (2026-10-21):

- ingest passes it through and stores it, possibly under options;
- egress writes it back out;
- the engine can navigate to it.

The forms such content takes, from "Extra elements" in `evidence/ingress-strictness/README.md`:

- fields from another FHIR version, such as R5's `triggeredBy` in R4 data;
- vendor or pipeline keys;
- `fhir_comments`;
- keys named like the layout's own fields.

It can also reach a stored table without going through ingest. Ticket 20 lists the routes: a merge that appended a field, another producer's file carrying `contained`, and a legacy `_fid` or `_extension` column. Egress currently throws on the first such column.

**What the specification has to settle:**

- **How it is stored and typed.** The ingress-strictness evidence sets out two parts:
  - D1, user-supplied definitions that describe the extras, so that their types still come from definitions;
  - D2, types inferred from the data, opt-in, with every such column marked as not FHIR.

  It rejected a JSON overlay column and VARIANT as the main direction, but either could be revisited if D2's drift proves unmanageable.
- **The options.** What the user chooses between, and what the default is. Dropping and reporting, as today, is one option.
- **Ingest.** How it fits whichever ingest mechanism ticket 16 settles. A key named like a layout field must still never be read as layout content.
- **Egress.** How stored extras are written back out, and what happens to a stored field that is neither described nor stored by ingest, which was ticket 20's case.
- **The engine.**
  - How FHIRPath navigates to the content, and what type it has.
  - How it is told apart from FHIR content.
  - How it interacts with `type()`, `ofType()` and views.
- **The costs recorded with D2.**
  - Schema drift between batches, append conflicts (FR-042), and widening to string.
  - Collisions when a later FHIR version defines the same name with another type.
  - An eager inference pass, which rules out streaming.
- **Dense mode.** What the dense schema does with the content.
- **The requirements it amends.** US1 scenario 5, FR-016 item 3, FR-025 and FR-063 each say that such content is not stored, or yields empty. FR-012 already names the D2 mode as a follow-up.

**Prior work:** An implementation of ticket 20 against the interim egress is parked, unpushed, on the local branch `issue/2830-egress-unknown-field` (`bead739730`). It leaves an unknown stored field out and reports it once per column, and it is tested for JSON and XML. Its copy of ticket 20 marks that ticket done, which is not so.

**Blocked by:** 11–15, by the owner's decision to follow the annotations. It is related to ticket 16, because the ingest mechanism decides how extras are carried.

**Status:** needs-info

- [ ] A specification is written under `specs/` and agreed with the owner.
- [ ] This ticket is replaced by that specification's tickets.
- [ ] The parked branch is landed or deleted, as the specification decides.
