# 45 — Settle the stored timestamp type once the specification is final

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004. Decision 138. Raised from the review of PR #2877, finding 3 (owner, 2026-10-29).

**What to build:** Once the specification's type for the range annotations is final (ticket 44), do one of the following, and amend decision 138 to match. Option B was added beside the other two (owner, 2026-11-16).

- **A. Keep `TIMESTAMP_MICROS`, and document the session setup** for platforms where the user supplies the session, Databricks first: the cluster's Spark configuration, `spark.sql.parquet.outputTimestampType TIMESTAMP_MICROS`, and its equivalent for a serverless or shared cluster, beside the session requirements in the Java, Python and R documentation.
- **B. Leave the range annotations out automatically** when the session is not set to `TIMESTAMP_MICROS`, rather than refusing the write.
- **C. Go back to Spark's default, `INT96`,** where the specification allows it. The sinks then accept a session at `INT96`, Pathling's own sessions no longer set `spark.sql.parquet.outputTimestampType`, and `StoredTimestamps`, `TimestampStorage` and the warning at context creation are removed or reduced to what is still needed. First check that `INT96` works throughout: the footer, comparisons and searches on stored dates, a Delta merge, and data written before and after the change read together.

**The impact on reused sessions.** Pathling refuses to write the range annotations from a session not set to `TIMESTAMP_MICROS`. A session the user brings is at Spark's default, `INT96`, so every existing pipeline that writes from one fails until it changes a setting. That covers a Databricks cluster, `spark-submit`, Python's active session and R's `pathling_connect(spark = sc)`. Record the impact on each for the option chosen.

**Two gaps, confirmed (2026-11-16).** Each is closed either way:

- There is no warning when the range annotations are off, although the data can still hold timestamps. Confirmed by reading the code: the check at context creation (`PathlingContext.java:201-212`) returns early, with no warning, when the range annotations are off, while the sinks' guard, `StoredTimestamps.requireMicros`, decides by the schema and the session alone.
- A raw Spark write, which does not go through Pathling's sinks, bypasses the `INT96` guard. Confirmed by a run: the guard is only in the library API's sinks (`ParquetSink:115`, `DeltaSink:129` and `CatalogSink:121`), not in `FhirWriter`, and a plain `write().parquet` with `spark.sql.parquet.outputTimestampType=INT96` writes `__start` and `__end` as `int96`.

**Blocked by:** 44, and the specification's agreement on the range annotations' type (ticket 46)

**Status:** needs-triage

- [ ] The specification's final type is recorded, with the link.
- [ ] One of options A, B and C is done, with its impact on reused sessions recorded, and decision 138 and `contracts/storage-layout.md` are amended to match.
- [ ] The two gaps are fixed or closed.
