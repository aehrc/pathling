# 28 — Release checklist

**Spec:** `specs/001-parquet-on-fhir/spec.md` — all success criteria. Old tasks: T138, T139; decision 129's cluster check.

**What to build:** The checks that gate the single Parquet on FHIR release.

**Blocked by:** 01–15, 17–19, 21–27, 29–33. Not 16, which decision 134 deferred: the release ships the interim ingest. Nor 34, which needs a specification of its own.

**Status:** ready-for-human

- [ ] On a Databricks cluster, files named `part-<n>-tid-<number>-<writeId>-…` are renamed with their partition id, write id and counter kept (decision 129, ticket 01).
- [ ] The full Python and R suites pass after the Ivy cache is cleared (`cache` and `jars` in every `~/.ivy2*` tree, then recreated).
- [ ] Every scenario in the spec's `quickstart.md` runs (T139).
- [ ] This programme's superseded change directories under `openspec/changes/` are removed or archived (T138).
- [ ] Every spec traceability row points at delivered work, and any gaps found are added to the owning ticket.
- [ ] The release notes state that XML is read without HAPI and accepts what JSON accepts, so a value HAPI rejected, such as an invalid date, is now stored, and that queries over it still work (tickets 31 and 32).
- [ ] Release-time gaps go on the owning ticket or here.
