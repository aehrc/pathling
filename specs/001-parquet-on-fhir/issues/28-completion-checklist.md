# 28 — Completion checklist

**Spec:** `specs/001-parquet-on-fhir/spec.md` — all success criteria. Old tasks: T138, T139; decision 129's cluster check.

**What to build:** The checks that close #2367. It is not by itself a release (owner, 2026-10-22): what the release needs beyond it is raised as follow-up issues by ticket 26.

**Blocked by:** None; retired (owner, 2026-11-16, see the comment below). Until then it was blocked by 01–15, 17, 21–27, 52, 29–32, 35, 41–46. Not 16, 18, 19, 38, 39 or 40, which the re-plan moved to GitHub issues after #2367 (owner, 2026-11-02), nor the defects of ticket 31 that the legacy line also has. Nor 33 or 34, moved to follow-up issues earlier. #2367 was to wait for the amendments to the Parquet on FHIR specification to be agreed (tickets 30, 35, 44 and 46).

**Status:** wontfix

- [ ] On a Databricks cluster, files named `part-<n>-tid-<number>-<writeId>-…` are renamed with their partition id, write id and counter kept (decision 129, ticket 01).
- [ ] The full Python and R suites pass after the Ivy cache is cleared (`cache` and `jars` in every `~/.ivy2*` tree, then recreated).
- [ ] Every scenario in the spec's `quickstart.md` runs (T139).
- [ ] This programme's superseded change directories under `openspec/changes/` are removed or archived (T138).
- [ ] Every spec traceability row points at delivered work, and any gaps found are added to the owning ticket.
- [ ] The release-note points are handed to the release's follow-up issue (ticket 26): XML is read without HAPI and accepts what JSON accepts, so a value HAPI rejected, such as an invalid date, is now stored; and the engine runs in `trust`, so a query over such a value has undefined behaviour (tickets 31 and 32).
- [ ] The amendments to the Parquet on FHIR specification are agreed, and Pathling matches the agreed text (ticket 46).
- [ ] The quantity's canonical value strips trailing zeros, so two equal quantities have byte-equal canonical values, as the proposed amendment requires (§8 of `evidence/upstream-amendments-draft.md`; decision 149, D6). A release gate (owner, 2026-11-05).
- [ ] The adoption test is met: two or three named implementations work with the specification, alongside ticket 46 (F2 in `evidence/annotations-decision-record.md`). A release gate (owner, 2026-11-05).
- [ ] The release notes say that a computed decimal changes from `DECIMAL(32,6)` to `DECIMAL(38,6)` (ticket 41), and record any change to a public return type from ticket 40 (owner, 2026-11-05).
- [ ] The requirements the re-plan deferred, such as dense mode, each have a GitHub issue (ticket 26).
- [ ] Gaps found at completion go on the owning ticket, here, or to ticket 26 as a follow-up issue.

## Comments

### Retired (owner, 2026-11-16)

There is no completion issue and there are no release gates. #2833 (stage 5) is retired, and each stage 5 ticket that is not a process step becomes its own GitHub issue, labelled `PoF_line`. The final release cut is chosen later from the open `PoF_line` issues, and the release process creates the release. This checklist therefore closes nothing, and its items are not carried over as gates.

H1, that 11 does not regress against 10.x, stays as a bullet in #2889 (Profile and optimise query performance across the engine), with ticket 10 (Benchmark: fix warm planning, add missing views, re-measure), to be weighed at the cut. The release-note points go into the body of the release pull request, #2896 (v11.0.0), at release time. The checklist above is kept as the record.
