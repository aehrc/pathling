# 20 — Egress ignores and reports a stored field it does not know

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-063. Old task: T124c. Ticket 16 may make this moot; check before starting.

**What to build:** Egress currently throws on a stored field that the canonical structure does not know. It should ignore and report the field instead, as ingest already does. Such a field can come from a merge that appended a field, from another producer's file carrying `contained`, or from a legacy `_fid` or `_extension` column. Today any of these fails the whole write on its first unknown column. The layout column (ticket 29) is not an unknown field: egress skips it without reporting it.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] If ticket 16 has landed or settled egress, confirm whether this still applies. Close it as moot with a comment if not.
- [ ] Tests write out a dataset carrying an unknown field: the write succeeds, the field is omitted and a finding is reported.
