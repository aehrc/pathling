# 43 — Evaluate annotations in the engine only where they are used

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-004, FR-022, FR-023. Raised from the review of PR #2877, findings 2 and 5 (owner, 2026-10-29).

**What to investigate:** how the engine processes annotations, and a design in which an annotation is evaluated lazily, only where an operation needs it. This is not the engine-wide performance review, which is ticket 26's profiling issue. It is a review of how annotations flow through the engine, and it should end with a chosen option.

What the review found:

- **A date's range is evaluated where only its text is used (finding 2).** A traversed date becomes a System date, `when(text.isNotNull(), struct(value, range))` in `SystemDateTime.of`, and on unannotated data the range is `date_range(text)`. A root date that is only output costs nothing, because the optimiser drops the unused range (`DateRangeComparisonTest.anOutputRootDateNeitherParsesNorReadsItsRange`). Under a repeating parent, or for a repeating date, the conversion is inside a lambda, where the range is not pruned, so a view or `$fhirpath` call that only outputs such a date parses every one of them. Ticket 26 lists this among its performance candidates.
- **A date comparison hides the stored bounds from pushdown (finding 5).** `TemporalComparator` and `DateMatcher`'s scalar path bind the range with `bindOnce`, `element_at(transform(array(range), ...), 1)`, even where the range is the stored structure or a literal. Removing the binding alone does not push anything down:
  - the System date's own `CASE WHEN` in `SystemDateTime.of` sits between the comparison and `_x.__start`, and Parquet's filter translation takes only a comparison on a plain attribute;
  - `TemporalComparator`'s body is itself a `CASE WHEN` that gives empty where the ranges cannot be compared, and Spark pushes no `CASE` to Parquet, so a pushed filter needs a separate conjunct on `__start` or `__end` that admits a superset of the matches (ticket 26);
  - `DateMatcher`'s scalar path already emits plain `And`, `Or` and comparisons, so it is the realistic first target.

  Do not replace `bindOnce` with `SqlFunctions.let` for a computed range: the range is a deterministic function, so `let` would inline it and parse the text once for each bound.

Consider:

- Where the decision to evaluate an annotation belongs: in the representation, so that a System date or decimal carries its text and a deferred annotation that only an operation needing it builds, or in Catalyst, so that unused fields are pruned through the expressions that hide them today.
- How it relates to ticket 42's choice between keeping a kind's fields together and caching their shared computation. A lazy annotation and a cached computation may be one design.
- The plan tests to start from: an output-only date under a repeating parent with no `date_range` in the optimised plan, and `birthdate=ge2000` and `where(birthDate >= @2000)` over annotated Parquet, asserting on the scan's pushed filters.

**Blocked by:** None. Related to 42, and decided together with it, first in stage 4 (owner, 2026-11-02).

**Status:** needs-triage

- [ ] The plan tests above are written, and show where annotations are evaluated today.
- [ ] The options are compared, with ticket 42, and the owner chooses one, which is recorded.
- [ ] The chosen option is implemented, or raised as a follow-up through ticket 26 if it outlives this programme.
