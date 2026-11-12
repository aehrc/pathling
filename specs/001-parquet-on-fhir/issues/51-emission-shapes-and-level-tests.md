# 51 — Emission shapes, level tests and the by-example tests

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002, FR-004, FR-022, FR-023. Split from ticket 43 (owner, 2026-11-11). Settled by the annotations design, `design/annotations-in-the-engine.md` §6 question 5 and §8, and decision 143. Examples: `design/annotations-by-example.md`. Evidence: `evidence/annotations-design-rewrites-layer1.md`, `evidence/annotations-design-pairing-shapes.md`, `evidence/annotations-design-project-before-generate.md`.

**What to build:**

- **Emission shapes (decision 143, L2).** Compute first, then generate (deferred until Pathling builds on a Spark that prunes through it; owner, 2026-11-12). Existence tests (`exists()`, `count()` and a multi-column `where`) are rewritten into the checked forms, with the guards the evidence lists, without which the rewrites differ from today's engine. A repeating primitive is paired as `arrays_zip(x AS value, _x.__start AS __start, _x.__end AS __end)`, so its fields match the singular form, with `coalesce(x, array())` guards on extracted arrays and never around `transform` (ticket 43 built the pairing, `ResolveOrNone`).
- **Level tests,** for dates and quantities, and for decimals once ticket 50 lands.
- **The by-example tests:** the examples of `design/annotations-by-example.md` §3 to §5.

**Blocked by:** 50.

**Status:** ready-for-agent

- [x] Plan-shape tests for the emission shapes assert the plan and equal results, in CI on Spark 4.0.2, including that the zipped fields carry the singular names and that the aliases add no projection that blocks pruning.
- [x] Engine tests cover each level, over annotated and unannotated data: a date and a quantity at the root, under a repeating parent (`Observation.component.valueQuantity`), inside an extension (an extension's `valueDate` and `valueQuantity`), and a repeating primitive under a repeating parent. Each gives the same results either way, reads the stored annotation where present, and computes nothing where only the value is output.
- [x] The examples of `design/annotations-by-example.md` §3 to §5 are covered by tests, except §4.4's System.Coding without `id`, which is ticket 53 (owner, 2026-11-12), and the switch-on examples of §3.1, which are ticket 52, and the engine suite passes over annotated data and over data written with every annotation disabled (`AnnotationFreeSuiteTest`).

## Comments

### Built, 2026-11-12 (branch issue/2832_51_emission-shapes)

**Compute first, then generate.** `Projection.execute` selects a view's result under an alias, then
`inline`s that column, so every `Generate` reads an attribute computed by a `Project` beneath it,
which the optimiser keeps.

**Existence forms.** `ColumnRepresentation.count()` and `isEmpty()`, which `exists()`, `count()`,
`empty()`, `exists(criteria)`, `where(...)` followed by any of them, and a view's `where` all reach,
take the size and the null test of `PresenceOf` (`encoders`, a `RuntimeReplaceable`) rather than of
the array itself: a criteria filter `filter(g, x -> c)` stands as
`filter(transform(g, x -> coalesce(c, false)), b -> b)` (E2); any other array of structures, the
null-removal filter included, stands as its first atomic field (E1); anything else as itself. Each
has the size and nullness of the array it stands for. Two deviations from the evidence's forms:
`exists(c)` and `empty()` use the same counted form as `count()` rather than `array_max`, which reads
the same fields and gives the same answer; and `all(c)` (E3) is not built, as the ticket and decision
143 do not name it. It inherits the counted forms through `where` and `count()`.

**Pairing.** Ticket 43's `ResolveOrNone` already zips a repeating primitive as decision 143 has it;
only tests were added.

**Tests.**

- `EmissionShapesTest` (29): existence results over data with an absent, an empty and an all-null
  array, null elements and elements whose read fields are null, recorded from the engine before the
  change; no `size`, `isnull` or `isnotnull` of an array of whole elements in any of their plans or
  in a view's `where` with `unionAll` (Spark's own inferred test of a generator's input is left
  out); every `Generate` reads an attribute over a `Project`; a repeating primitive's zipped fields
  carry the singular names, its annotation arrays are extracted and guarded with no alias and no
  guard around a `transform`, and over data that stores the primitive's extensions the read leaves
  them out, through the engine and through a view. That read is the one pruned read asserted: on
  Spark 4.0.2 nothing is pruned through a lambda, so elsewhere only the shape is asserted.
- `PresenceOfTest` (7): each form's shape, and equal size and nullness in every row, including a
  criteria with an index argument and one that reads an outer lambda variable.
- `AnnotationLevelsTest` (22): a date and a quantity at the root, under a repeating parent and in an
  extension, a decimal at the root and under a repeating parent, and a repeating date under a
  repeating parent; each comparison gives the same results over annotated and unannotated data,
  computes nothing over annotated data and computes over unannotated data; each output-only date
  and quantity computes nothing over either, and a quantity output as a caller is given it computes
  no canonical form at the root, under a repeating parent or in an extension.
- `AnnotationsByExampleTest` (25): the forms of `design/annotations-by-example.md` §3 to §5 over
  its §1 resource, apart from the gaps below.

No existing test's expectation changed. The engine suite is the unannotated run, as before, and
`AnnotationFreeSuiteTest` passes within it.

**Test runs.** `mvn verify -pl library-api,legacy-export -am` at the final commit, with no skips,
all green: utilities 40, fhir-schema 101, encoders 627, io 980 (1 skipped), legacy-export 38,
terminology 926, fhirpath 8204 (888 skipped), library-api 381. The reactor built every upstream
module from this branch. Python, R and the server were not run.

**Awaiting the owner.**

1. **Compute-first evaluates a view's result three times.** Once the generator reads an attribute,
   Spark's `InferFiltersFromGenerate` adds `size(e) > 0 AND isnotnull(e)` beneath the projection, with
   the whole result expression `e` repeated in it, so each row computes it in the filter twice and in
   the projection once. Before the change no filter was inferred. On Spark 4.0.2 the change prunes
   nothing in return (decision 143), and its gain is on Spark 4.3. Not measured. Options: (a) keep it
   and measure in ticket 10's benchmark; (b) measure now, on the benchmark's views, before merging;
   (c) leave compute-first out until Pathling builds on a Spark that prunes through it. Recommended:
   (b), and (c) if it is slower.
2. **§4.4 System.Coding.** The engine's System.Coding still carries `id`, as
   `contracts/engine-semantics.md` and `SystemCodingShapeTest` have it, while decision 150 and §4.4
   drop it. No ticket builds the drop. Options: (a) build it here, which changes
   `SystemCodingShapeTest`'s expectation; (b) leave it to #2804 or a new ticket, and amend box 3 to
   exclude §4.4. Recommended: (b). Box 3 is not ticked for this.
3. **The output of a FHIR Quantity.** §5 says a complex FHIR value is output in its fitted shape, but
   a stored Quantity reaches a caller as a four-field System.Quantity (ticket 50); a Coding is output
   in its fitted shape. Options: (a) amend §5 to say so; (b) output a stored Quantity in its fitted
   shape. Recommended: (a).
4. **A decimal that is only output** is decoded over unannotated data, because traversal tests
   whether its number decodes (§3.1) and a view outputs the number until ticket 16; so is a
   quantity's value. Over annotated data nothing is computed, and no canonical form is computed for
   an output quantity at any level. Options: (a) accept it as the box's exception, pinned by
   `AnnotationLevelsTest`; (b) defer the decodability test to where the number is read. Recommended:
   (a).

The switch-on examples of §3.1 are ticket 52's, and are not tested here.

**Follow-up for ticket 26.** `toDate()` on a dateTime with a time, such as
`effectiveDateTime.toDate()`, returns empty. §4.1's example and, as far as can be told without
checking, the FHIRPath specification convert a DateTime to its date. The by-example test uses
`'2024-03-15'.toDate()` instead.

### Owner's decisions, 2026-11-12

1. **Compute-first is left out** until Pathling builds on a Spark that prunes through it. It is
   removed from `Projection.execute`, and the plan-shape tests no longer assert it. Why: once the
   generator reads an attribute, Spark infers `size(e) > 0 AND isnotnull(e)` beneath the projection,
   evaluating the view's whole result three times a row, and on Spark 4.0.2 the shape prunes nothing
   in return. Recorded in design §8 and as an amendment to decision 143, for the Spark upgrade to pick
   up. The existence forms and the pairing stay.
2. **System.Coding drops `id`** here, per decision 150 and §4.4, with `SystemCodingShapeTest`'s
   expectation approved to change. Not yet built: see the question below.
3. **The output of a stored Quantity** is the four-field System.Quantity; `annotations-by-example.md`
   §5 is amended, and `AnnotationsByExampleTest` asserts it.
4. **A decimal that is only output**, a quantity's value included, is decoded over unannotated data,
   accepted as box 2's exception and pinned by `AnnotationLevelsTest`.

**Question, before building the System.Coding drop.** The engine's System.Coding is
`CodingSchema.DATA_TYPE` in `terminology`, which is also the return type of the terminology UDFs that
the library API, Python and R call directly, and the shape `TerminologyHelpers.toCoding` builds.
Dropping `id` there changes protected expectations above the FHIRPath layer:
`library-api` `TerminologyHelpersTest.toCoding` (positional field assertions) and Python
`tests/test_udfs.py` (`CodingRow` with `id`, compared against `translate()` results); R's UDF tests
may follow. Options: (a) drop `id` from `CodingSchema` everywhere, changing those expectations and the
documented six-field shape in `contracts/engine-semantics.md` and `contracts/library-api.md`; (b) drop
it only from the engine's System.Coding (literals, terminology results and quantity units inside
FHIRPath), leaving the library-level UDF and `toCoding` shape with `id`, so only
`SystemCodingShapeTest` changes. Recommended: (b), which is decision 150's scope (engine System
types); the library shape could follow in its own change.

**Follow-up for ticket 26.** `toDate()` on a dateTime with a time, such as
`effectiveDateTime.toDate()`, returns empty, where §4.1's example, and the FHIRPath specification as
far as was checked without the spec text, convert a DateTime to its date.

### System.Coding, 2026-11-12

The `id` drop is not built in this ticket (owner, 2026-11-12). It is raised as ticket 53 (Drop `id`
from System.Coding), with the two scopes and the protected expectations each touches, and box 3 is
amended to exclude it. `SystemCodingShapeTest` is unchanged. With that, every other §3 to §5 example
is covered by `AnnotationsByExampleTest` or the tests it names, and box 3 is ticked.
