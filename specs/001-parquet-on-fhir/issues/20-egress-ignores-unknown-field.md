# 20 — Egress ignores and reports a stored field it does not know

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-063. Old task: T124c. Ticket 16 may make this moot; check before starting.

**What to build:** Egress currently throws on a stored field that the canonical structure does not know. It should ignore and report the field instead, as ingest already does. Such a field can come from a merge that appended a field, from another producer's file carrying `contained`, or from a legacy `_fid` or `_extension` column. Today any of these fails the whole write on its first unknown column. The layout column (ticket 29) is not an unknown field: egress skips it without reporting it.

**Blocked by:** 16, the choice of ingest and egress mechanism that decision 134 deferred

**Status:** needs-info

- [ ] If ticket 16 has landed or settled egress, confirm whether this still applies. Close it as moot with a comment if not.
- [ ] Tests write out a dataset carrying an unknown field: the write succeeds, the field is omitted and a finding is reported.

## Comments

### Deferred until the ingest and egress mechanism is chosen (2026-10-21)

Owner decision. Egress may be rewritten by ticket 16, which would settle how a stored field it does not know is
treated, so this waits for that choice rather than fixing the interim egress. An implementation against the
interim is parked, unpushed, on the local branch `issue/2830-egress-unknown-field` (`bead739730`): it leaves
the field out and reports it once per column, and is tested for JSON and XML. When the choice is made, check
whether it still applies, then land it or close this ticket as moot.
