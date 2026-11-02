# 45 — Settle the stored timestamp type once the specification is final

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004. Decision 138. Raised from the review of PR #2877, finding 3 (owner, 2026-10-29).

**What to build:** Once the specification's type for the range annotations is final (ticket 44), do one of the following, and amend decision 138 to match.

- **Go back to Spark's default, `INT96`,** where the specification allows it. The sinks then accept a session at `INT96`, Pathling's own sessions no longer set `spark.sql.parquet.outputTimestampType`, and `StoredTimestamps`, `TimestampStorage` and the warning at context creation are removed or reduced to what is still needed. First check that `INT96` works throughout: the footer, comparisons and searches on stored dates, a Delta merge, and data written before and after the change read together.
- **Keep `TIMESTAMP_MICROS`, and document the session setup** for platforms where the user supplies the session, Databricks first: the cluster's Spark configuration, `spark.sql.parquet.outputTimestampType TIMESTAMP_MICROS`, and its equivalent for a serverless or shared cluster, beside the session requirements in the Java, Python and R documentation.

**Blocked by:** 44, and the specification's agreement on the range annotations' type (ticket 46)

**Status:** needs-triage

- [ ] The specification's final type is recorded, with the link.
- [ ] One of the two options is done, and decision 138 and `contracts/storage-layout.md` are amended to match.
