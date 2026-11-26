# Specification Quality Checklist: Load FHIR resources with a schema defined up front

**Purpose**: Validate that the specification is complete and ready for planning.
**Created**: 2026-11-25
**Feature**: [spec.md](../spec.md)

## Content quality

- [x] CHK001 No implementation detail (classes, modules, algorithms). The spec names only the
      public surface the user sees (sources, `encode`, `encodeBundle`), which is the subject of
      the feature.
- [x] CHK002 Focused on user value: each story names who it serves and why.
- [x] CHK003 All mandatory sections complete.

## Requirement completeness

- [x] CHK004 No `[NEEDS CLARIFICATION]` markers remain. The one deferred decision (absent resource
      types) is resolved under Open Items: today's behaviour, with #2997 as the follow-up.
- [x] CHK005 Requirements are testable and unambiguous: each FR states an observable behaviour.
- [x] CHK006 Success criteria are measurable and technology-agnostic beyond the public API names.
- [x] CHK007 Acceptance scenarios are defined for every story.
- [x] CHK008 Edge cases are identified: invalid schema (refused by the check #2884 adds), field
      order other than canonical, annotation and metadata fields, absent and unnamed types,
      unreadable schemas, unknown keys, empty set, Delta merge narrowing, differing append, stored
      type clashes, and content dropped silently by text and streaming input.
- [x] CHK009 Scope is bounded: Out of Scope lists the two follow-ups of FR-025 and the excluded work.
- [x] CHK010 Assumptions are recorded.

## Feature readiness

- [x] CHK011 Every FR has acceptance criteria: FR-001–010 → US1 and US4 (FR-011 removed), with
      FR-006a's debug log tested by T067; FR-012–013 → US2 (FR-014 removed); FR-015–016 removed
      with US3; FR-017–019 → US4; FR-020–021b → SC-005, SC-006; FR-022–024 → US2 scenario 4,
      SC-008; FR-027 → US4, SC-009; FR-028 → US4, SC-007; FR-022a, FR-025 and FR-026 are delivery
      obligations checked at completion.
- [x] CHK012 User scenarios cover the primary flows: batch load and write, streaming encode
      written through a Pathling sink in `foreachBatch`, schemas from data.

## Notes

- First pass found FR-025 and FR-026 without an acceptance scenario. They are process obligations
  (issues filed, cross-references updated), so they are verified by tasks rather than by a test.
