# 38 — Consider deciding the annotation step's *fill* per row

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-021, FR-022, FR-023; decision 135. Raised from ticket 11's first review item.

**What to decide:** whether the annotation step's *fill* mode treats a null stored annotation in one row as missing and computes it, rather than keeping it because the schema carries the field.

Ticket 11 decides *fill* per schema, and the owner kept that for now (2026-10-22). Where the schema carries an annotation, the step keeps it as stored, including in a row where it is null. The step's resolver then follows the same rule as the engine's, which FR-023 requires to decide from the schema.

A dataset merged from annotated and unannotated sources is where the two readings differ. For example, January's Patients were encoded with the range kind on, and February's were written without it. Read together, the schema carries `_birthDate.__start` and February's rows hold a null in it. Per schema, `annotate` leaves those nulls in place, and only *recompute* repairs them, which also recomputes January's rows. Per row, the step would take the stored value where present and compute it otherwise (`coalesce(stored, computed)`), a small change in `Annotator.resolver`.

This matters beyond repair. *Fill*, the default, can make the engine silently wrong on merged data: the engine reads a stored annotation wherever the schema carries it, so a null annotation beside a non-null value takes the null fast path and answers as if the value were absent.

The engine stays per schema either way (FR-023): computing per row in a query pays for the computation on every row, which defeats storing the annotation. The question is only whether the step, which runs at write time, should differ from the engine here.

Decide it once real kinds exist (tickets 12–14), so that the merged-dataset case can be shown with one of them.

**Blocked by:** 11, and at least one of 12–14

**Status:** ready-for-human

- [ ] The owner decides per row or per schema for the step's *fill*, and the decision is recorded.
- [ ] If per row: the step computes an annotation for a row whose stored value is null and keeps a non-null one, pinned by a test over a dataset merged from an annotated and an unannotated source, and the configuration docs in Java, Python and R say so.
- [ ] If per schema: the configuration docs say that *fill* keeps a null stored annotation, and that *recompute* repairs a merged dataset.
