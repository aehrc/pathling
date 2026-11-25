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
      types) is recorded under Open Items with a provisional answer, not left unanswered.
- [x] CHK005 Requirements are testable and unambiguous: each FR states an observable behaviour.
- [x] CHK006 Success criteria are measurable and technology-agnostic beyond the public API names.
- [x] CHK007 Acceptance scenarios are defined for every story.
- [x] CHK008 Edge cases are identified: invalid schema, annotation and metadata fields, absent and
      unnamed types, unreadable schemas, unknown keys, empty set, Delta merge narrowing, differing
      append.
- [x] CHK009 Scope is bounded: Out of Scope lists the three follow-ups and the excluded work.
- [x] CHK010 Assumptions are recorded.

## Feature readiness

- [x] CHK011 Every FR has acceptance criteria: FR-001–010 → US1 and US4 (FR-011 removed); FR-012–014 → US2;
      FR-015–016 → US3; FR-017–019 → US4; FR-020–021b → SC-005, SC-006; FR-022–024 → US2 scenario 5,
      SC-008; FR-025–026 are delivery obligations checked at completion.
- [x] CHK012 User scenarios cover the primary flows: batch load and write, streaming encode,
      destination check, schemas from data.

## Notes

- First pass found FR-025 and FR-026 without an acceptance scenario. They are process obligations
  (issues filed, cross-references updated), so they are verified by tasks rather than by a test.
