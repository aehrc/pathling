# 44 — Reconsider keeping INT96 in the range annotations' amendment

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004. Decision 138; [aehrc/parquet-on-fhir#3](https://github.com/aehrc/parquet-on-fhir/issues/3). Raised from the review of PR #2877, finding 3 (owner, 2026-10-29).

**What to build:** A revision of the upstream amendment raised by ticket 14, which proposes that `start` and `end` be `int64` with `TIMESTAMP(isAdjustedToUTC=true, MICROS)`. The revision considers keeping `int96`, the specification's physical type and Spark's default, as an option the specification allows.

Under decision 138, Pathling refuses to write the range annotations from a session not set to `TIMESTAMP_MICROS`. A session the user brings, such as a Databricks notebook's or one passed to `PathlingContext.create(spark)`, is at `INT96` by default, so every existing pipeline that writes from one fails until it changes a Spark setting. Keeping `int96` in the specification would let such a session write as it is.

Weigh, with evidence where the specification's choice rests on it:

- What `int96` costs: no statistics and no predicate pushdown (decision 138), no logical type, and its deprecation in Parquet.
- What it keeps: the microsecond bounds, so comparisons give the same results as `int64` microseconds (`scripts/ticket-14/range-type-probe.txt`), and no setting for a user's session.
- Whether the specification should allow both, `int96` and `int64` `TIMESTAMP(isAdjustedToUTC=true, MICROS)`, and what a reader then does.

**Do not raise anything on GitHub.** Write the revision, as a comment on aehrc/parquet-on-fhir#3, to `.local/work/<branch>/pof-spec-range-type-int96.md`. The owner reviews it, and the parent session posts it.

**Blocked by:** None

**Status:** needs-triage

- [ ] The revision is drafted, with the costs of each type and a recommendation.
- [ ] It is posted on aehrc/parquet-on-fhir#3 after the owner's review, and the link is recorded in this ticket's comments.
