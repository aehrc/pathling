# 28 — Completion checklist

**Spec:** `specs/001-parquet-on-fhir/spec.md` — all success criteria. Old tasks: T138, T139; decision 129's cluster check.

**What to build:** The checks that close #2367. It is not by itself a release (owner, 2026-10-22): what the release needs beyond it is raised as follow-up issues by ticket 26.

**Blocked by:** 01–15, 17–19, 21–27, 29–32, 41. Not 16, which decision 134 deferred: #2367 delivers the interim ingest. Nor 33, moved to the follow-up. Nor 34, moved to a follow-up issue, nor 35: #2367 does not wait for its amendments to be agreed upstream.

**Status:** ready-for-human

- [ ] On a Databricks cluster, files named `part-<n>-tid-<number>-<writeId>-…` are renamed with their partition id, write id and counter kept (decision 129, ticket 01).
- [ ] The full Python and R suites pass after the Ivy cache is cleared (`cache` and `jars` in every `~/.ivy2*` tree, then recreated).
- [ ] Every scenario in the spec's `quickstart.md` runs (T139).
- [ ] This programme's superseded change directories under `openspec/changes/` are removed or archived (T138).
- [ ] Every spec traceability row points at delivered work, and any gaps found are added to the owning ticket.
- [ ] The release-note points are handed to the release's follow-up issue (ticket 26): XML is read without HAPI and accepts what JSON accepts, so a value HAPI rejected, such as an invalid date, is now stored; and the engine runs in `trust`, so a query over such a value has undefined behaviour (tickets 31 and 32).
- [ ] Gaps found at completion go on the owning ticket, here, or to ticket 26 as a follow-up issue.
