# 43 — Evaluate annotations in the engine only where they are used

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002, FR-004, FR-021, FR-022, FR-023, FR-059. Raised from the review of PR #2877, findings 2 and 5 (owner, 2026-10-29). Settled by the annotations design, `design/annotations-in-the-engine.md` §4.1 to §4.5 and §8, and decisions 140 to 144, 147, 151 and 152. Examples: `design/annotations-by-example.md`.

**What to build:** The accessor model. Nothing is computed when a value is traversed. An operation that needs an annotation asks for it through an accessor, which reads the stored field or computes it where the schema lacks it; Spark makes that choice when it resolves the plan, so the engine stays schema-agnostic. The review's findings are what this fixes: a date's range is evaluated where only its text is output (finding 2), and a date comparison hides the stored bounds from pushdown (finding 5).

- **Element form (decisions 141 and 152).** A FHIR primitive whose type has an annotation kind is `struct(value, <named stored annotation fields>)`, flat: `__start` and `__end` for a date, dateTime or instant, and `__numeric` for a decimal. The shape is fixed by the type, not the data, so a date over unannotated data is `struct(value)`. The raw `_x` is never carried. A repeating primitive is paired as `arrays_zip(x AS value, _x.__start AS __start, _x.__end AS __end)`, so its fields match the singular form, with `coalesce(x, array())` guards on extracted arrays and never around `transform`.
- **Accessors (design §4.3).** Each annotation is read from the element itself, such as `resolve_or_else(e, "__start", date_start(e.value))`, over ticket 42's memoised UDFs. `SystemDateTime`'s eager range goes. A decimal keeps its stored text as `value`; its number is `numeric(e)`, the stored `__numeric` or else the parsed text, deferred until it is read.
- **Conversion (decision 142, D8, as clarified 2026-11-09).** Every operation that accepts System types converts through one shared step, `resolve_or_none(e, [annotation fields])` plus the System type's content fields. It keeps the annotation fields present, drops `id`, `extension` and any other content the System type lacks, and computes no annotation. `ResolveOrNone` is a new `RuntimeReplaceable` beside `ResolveOrElse` and `ResolveOrNull`. For dates the step replaces today's eager conversion. For decimals it is new, because traversal yields the number today, so each place that uses `DecimalCollection`'s column directly gains it.
- **Decimal detection (decision 147).** A decimal is classed as stored or computed from the collection's definition, as `QuantityCollection.holdsStoredQuantities` does. A union is stored only if every operand is.
- **Merges (decision 142, D9, and decision 152).** `MergeCast` and `UnresolvedMergeCombination` keep only the annotation fields every operand has, and drop `id` and `extension` for a System result. An operation that converts before it compares, such as a merge or `toQuantity()`, keeps the annotation fields it has; where they are absent the accessor computes from the converted value.
- **Comparison keys (decision 151).** The whole-struct `array_distinct` and `array_union` in `CombiningLogic` and `Collection` are replaced by deduplication on a key computed once per element, keeping the first element:
  - a primitive's key is its System value, whatever its annotation fields;
  - Quantity and Coding keep main's semantics, through `QuantityComparator` and `CodingEquality`;
  - another complex type compares as its element struct, with another producer's `__` fields stripped at every depth by a projection planned from the schema; nested `id` and `extension` stay.
- **Emission shapes (decision 143, L2).** Compute first, then generate. Existence tests (`exists()`, `count()` and a multi-column `where`) are rewritten into the checked forms.
- **Pushdown (decision 144).** A date comparison that is empty where it cannot compare is emitted beside a plain superset conjunct on the stored bound, such as `CASE WHEN canCompare THEN start >= ts END AND __end >= ts`, in `TemporalComparator` and in `DateMatcher`'s scalar path. `bindOnce` leaves both.
- **Output (decision 142, D10, and design §4.5).** A primitive leaves the engine as its value only. Its annotation fields are never output, and neither are a System.Quantity's canonical fields.

**Not in this ticket:**

- The D14 switch for a primitive's `id` and `extension` is ticket 17 (owner, 2026-11-10). Until then the element has no content fields beside `value`.
- Shaping elements by use (L1, decision 140) and Pathling's own optimiser rule (D3b, decision 143; #2890).
- Presence-driven algorithm choice (decision 148), and which form is the truth for a computed System value (design §6 question 6; #2892). Rule A holds meanwhile: an operation that produces a new value produces no annotations.
- Grouped annotations, parked as design §6 question 1 (owner, 2026-11-10).

**Blocked by:** 42, for the baseline plan tests and `date_start` and `date_end`.

**Status:** ready-for-agent

- [ ] Ticket 42's baseline plan tests reach their target, and each change from the baseline is explained: an output-only date under a repeating parent, and after `where()`, has no date computation in the optimised plan; the merges of different shapes give the results of `design/annotations-by-example.md` §5.1; `birthdate=ge2000` and `where(birthDate >= @2000)` over annotated Parquet push a filter on `__end` to the scan.
- [ ] Plan-shape tests for the emission shapes assert the plan and equal results, in CI on Spark 4.0.2, including that the zipped fields carry the singular names and that the aliases add no projection that blocks pruning.
- [ ] A test runs each annotated type over annotated and unannotated data and gets the same results, including values whose text and number order differently (`"10"` against `"9"`, `1.0` against `1.00`).
- [ ] Comparison-key tests: another producer's `__` field in a HumanName does not affect `=`, `distinct()` or `|`; a primitive's annotation fields do not either; Quantity and Coding behave as on main.
- [ ] `SystemDateTime` builds no eager range, and `TemporalComparator` and `DateMatcher` contain no `bindOnce`.
- [ ] The examples of `design/annotations-by-example.md` §3 to §5 are covered by tests, and the engine suite passes over annotated data and over data written with every annotation disabled (`AnnotationFreeSuiteTest`).
