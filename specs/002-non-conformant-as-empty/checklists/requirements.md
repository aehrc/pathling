# Requirements Checklist: Non-conformant data reads as empty

**Purpose**: Check the specification's quality before planning.
**Created**: 2026-11-24
**Feature**: [spec.md](../spec.md)

## Content quality

- [x] CHK001 No implementation detail beyond what the behaviour is defined by. Spark's non-ANSI cast
      semantics are named in FR-005 because they define which conversions succeed (owner, 2026-11-24), not as a
      choice of mechanism.
- [x] CHK002 Focused on user value: loads and queries that do not fail, and correct answers on conformant
      data.
- [x] CHK003 Mandatory sections complete.

## Requirement completeness

- [x] CHK004 No `[NEEDS CLARIFICATION]` markers. Three questions are deliberately open (OQ-A, OQ-B, OQ-C),
      each with its options, the story it gates and a spike to settle it (owner, 2026-11-24, option (a) of the
      delivery question).
- [x] CHK005 Requirements are testable and unambiguous. FR-014, FR-017 and the second half of FR-012
      become testable once their open question is decided.
- [x] CHK006 Success criteria are measurable. SC-005 is measured against the benchmark's own noise.
- [x] CHK007 Acceptance scenarios defined for every story; edge cases listed.
- [x] CHK008 Scope bounded by an explicit out-of-scope list.
- [x] CHK009 Assumptions recorded.

## Feature readiness

- [x] CHK010 Every functional requirement has acceptance criteria: FR-001 to FR-003 (US1), FR-004 to FR-006
      (US2), FR-007 and FR-008 (US3), FR-009 to FR-013 (US2, US4), FR-014 (US5), FR-015 (US1 to US4, through the
      tests), FR-016 (US4), FR-017 (US6), FR-018 and FR-019 (US1).
- [x] CHK011 User scenarios cover the primary flows: ingest of JSON and XML, querying ingested data, and
      querying data from other writers.

## Notes

- All items pass. The open questions are not gaps: the owner chose to build the settled stories first and
  settle each question by a spike.
