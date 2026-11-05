# Requirements Checklist: Parquet on FHIR

**Purpose**: Validate that `spec.md` is complete, unambiguous and free of
implementation detail before planning begins.
**Created**: 2026-09-14
**Feature**: [spec.md](../spec.md)

## Content quality

- [x] CHK001 No implementation detail in the requirements — no class, module,
      library or Spark API named in `spec.md`. *Checked: the requirements speak
      of "the definition abstraction", "file-based sources", "the query-time
      expression toolkit" rather than naming types. The Parquet on FHIR
      specification is named because it is the subject, not an implementation
      choice.*
      **Three justified exceptions**, each a requirement about a specific
      external artefact that cannot be stated without naming it, in the same
      way SC-009 names the build:
      FR-048 and FR-049 name Spark's internal API, because being free of it
      *is* the requirement (driver 5);
      FR-049 distinguishes dependency-level from import-level enforcement,
      because the two give materially different guarantees and the weaker one is
      only acceptable where the stronger is impossible;
      FR-051 names Maven coordinates, because "existing consumers keep
      resolving" is the obligation and coordinates are what they resolve;
      FR-055 names Spark's type system (the bottom type, arrays of it), because
      the representation of an absent element *is* the requirement, and the
      measurements show no other representation satisfies FR-027.
- [x] CHK002 Focused on user value and observable behaviour.
- [x] CHK003 All mandatory sections present: user scenarios, requirements,
      success criteria, assumptions.
- [x] CHK004 Written so a reader who does not know the codebase can follow it.

## Requirement completeness

- [x] CHK005 No `[NEEDS CLARIFICATION]` markers remain.
- [x] CHK006 Every requirement is testable. *Checked one by one. FR-049 ("no
      expression encoder, no hand-authored expression tree, no FHIR object in a
      per-row plan") is verified by inspection rather than execution; recorded
      as such in US8's independent test.*
- [x] CHK007 Requirements are unambiguous — each states a single obligation.
- [x] CHK008 Success criteria are measurable.
- [x] CHK009 Success criteria are technology-agnostic. *SC-009 names the build
      and a Spark component; kept, because the module boundary is a requirement
      about a specific external dependency and cannot be stated without it.*
- [x] CHK010 Acceptance scenarios are defined for every user story.
- [x] CHK011 Edge cases are identified, including the ones discovered during
      the interview: all-null structures serialising as empty objects, null-only
      arrays, untyped columns escaping to callers, and lexical decimals on the
      string-dataset path.
- [x] CHK012 Scope is bounded — the server, the execution path's Catalyst
      dependency, profile support and non-R4 versions are all excluded in
      Assumptions.
- [x] CHK013 Assumptions are recorded, including the accepted limitation.

## Feature readiness

- [x] CHK014 Every functional requirement is reachable from at least one
      acceptance scenario or success criterion.
- [x] CHK015 User scenarios cover the primary flows: storage, round trip, query,
      sparse schemas, merging, primitive metadata, detection, module boundary.
- [x] CHK016 Stories are independently testable and independently valuable.
- [x] CHK017 Priorities assigned, with reasoning.

## Findings and revisions

- CHK001 initially failed: an early draft named `EmptyCollection`,
  `ProjectedColumn` and `VariantBuilder` in the requirements. Rewritten to
  describe the behaviour; the mechanisms moved to `plan.md` and `research.md`.
- CHK006 initially failed for two requirements phrased as design statements
  ("the engine keeps a handle on the parent struct", "the pre-pass quotes
  decimal tokens"). Both were mechanism, not requirement; replaced by FR-034 and
  FR-020 respectively.
- CHK011 initially failed: the empty-object and null-only-array cases existed
  only in the research notes. Promoted to edge cases and to FR-019.
- CHK014 initially failed for FR-023 and FR-036, which had no scenario.
  US3 scenario 2 and US5 scenario 2 now cover them.
- A requirement that the row-drop mitigation be implemented was **removed**: the
  interview established that Pathling's engine is not exposed, so the obligation
  is FR-036 (do not become exposed) rather than a mitigation to build.
- Second pass, after the tasks were written:
  - FR-017 (bounded losslessness on a dense schema) had no task. Added T078a
    and T078b.
  - FR-052 and FR-053 were satisfied only implicitly. T128 and T100 now state
    them.
  - US2's strictness scenarios belonged to ingest, not to the round trip. Moved
    to US1, and US2 gained the dense-bounds scenario that FR-017 needs.
  - An inference hazard survived the correction of the design document's
    argument: conformant FHIR JSON always represents a repeating element as an
    array, so inference gets cardinality right — but *non-conformant* input
    supplying a repeating element as a single object does not, and the transform
    must route that to the strictness switch rather than coerce it. Added T057a.
  - T078 and T108 are pins rather than red-green tests; the tasks preamble now
    names that category so the implementation phase does not hunt for a way to
    make them fail first.

- Third pass, after the schema-binding decision:
  - The requirements assumed an expression could be pruned against a known
    schema when it was built. It cannot: the expression-to-column API converts an
    expression to a column with no dataset in hand, so a statically pruned column
    is valid only for the schema it was built against and returns empty on a
    wider dataset that *does* carry the element. FR-054 now states the contract
    that was previously only implicit.
  - FR-030 was inverted. It required an untyped column to be documented; it now
    requires an absent primitive to carry its definition-derived type, which the
    chosen mechanism supplies for free. US4 scenario 5, the engine-semantics type
    table, the library-API behaviour-change section and T107/T108 all followed.
  - CHK011 gained three edge cases the interview had not reached: two collections
    of one FHIR type with different fitted shapes; structures carrying the same
    fields in a different order; and the positional-equality hazard that makes the
    second of those a silent wrong answer rather than a failure. FR-056 to FR-058
    cover them.
  - T108 was reclassified out of the pins category, since under FR-054 it is a
    red-green test rather than a record of an accepted consequence. The pins list
    in the tasks preamble now names T078 alone.
