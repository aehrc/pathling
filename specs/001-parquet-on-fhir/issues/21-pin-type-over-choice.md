# 21 — Pin FHIRPath type() over a choice element

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-013. Old task: T124d.

**What to build:** Engine tests pinning `type()` over a choice element, including one that can hold a reference, such as `Extension.value`. M1 changed `FhirChoiceDefinition.getAllChildTypes` to declared order with one reference variant. That method also feeds `TypeInfo.choiceTypeInfoMapper`, so engine behaviour moved with no test to show it.

**Blocked by:** None — can start immediately

**Status:** done

- [x] Tests cover `type()` over a non-reference choice and a reference-capable choice.
- [x] The same expectations are checked against the pre-M1 definition code, for example the `main` tag before the programme. Any disagreement is reported to the owner.

## Comments

### Implementation (branch `issue/2830-type-over-choice`)

The tests pin the current behaviour, which matches the specification in every case. The release
before the programme does not agree: `v9.9.0` fails with an analysis error on every
reference-capable case, and on the non-reference controls beside them too. One engine change
followed review: a plural `type()` no longer gives a null entry for an absent value (see below).

**Accepted by the owner (2026-10-21).** Both differences from `v9.9.0` are intended: a choice that
can hold a reference, or that sits at the resource root, now gives the specification's type where
it failed, and a plural `type()` over a choice now leaves out a member with no value. The second
changes the answer to an expression that worked before, such as `component.value.type().count()`,
so it belongs in the release notes. It is the defect raised as #2860, which this branch fixes.

Three methods in `TypeFunctionsDslTest` pin `type()` over a choice that can hold a reference. Each
covers a Reference value, a non-reference value and an absent value:

- `testTypeFunctionOnOpenChoiceThatCanHoldReference` uses `Patient.extension.value`, an open choice.
  The extensions are a `valueReference`, a `valueString` and a complex extension with no value, in
  that order. Each is reached singly by index (`extension[0].value.type()`) and through
  `extension('<url>')`. The plural collection is tested by position (`extension.value.type()[0]`
  and `[1]`), by the names of the extensions that carry a value, and by filtering on
  `value.type().name`.
- `testTypeFunctionOnClosedChoiceThatCanHoldReference` uses `CarePlan.activity.detail.product`, a
  closed choice of `CodeableConcept | Reference(Medication | Substance)` inside a repeating
  element. It uses the same singular and plural cases.
- `testTypeFunctionOnRootChoicesThatCanHoldReference` uses `MedicationRequest.medication` and
  `MedicationRequest.reported`. Both are singular closed choices at the resource root, and so are
  scalar columns. The cases are Reference, CodeableConcept, boolean and absent.

The expected values follow the specification's reflection section and the FHIR binding: a Reference
value gives `FHIR.Reference(FHIR.Element)`, and every other type gives `FHIR.<type>(FHIR.Element)`.
An absent value gives no result (`testEmpty`). In the plural cases the absent member comes last, so
that the positions of the others do not depend on whether an absent value is counted (see the
observation below). fhirpath.js 5.1.0 behaves the same way: its `typeFn` maps `TypeInfo.fromValue`
over each item of the input. The non-reference cases already pinned for
`Observation.component.value` were not duplicated.

#### Comparison with the code before the programme

The library release before the programme (which branched from `main` at `44be98ee18`) is `v9.9.0`.
To compare, I made a detached worktree at that tag and applied the same three methods to its
`TypeFunctionsDslTest` without changing them. The DSL there is the same as on this branch. I ran
`mvn package -pl fhirpath -am -Dtest=TypeFunctionsDslTest`, and then removed the worktree.

On valid data at most one variant of a choice is present per row. So the change from hash order to
declared order could not have changed any answer, and agreement was the expected result. What could
differ was the old variant list itself, which `getValidChildNames()` supplied: the old mapper read
each name with an unguarded `getField`. The results:

| Cases                                                                              | `v9.9.0`                                                                                | This branch | Specification       |
| ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | ----------- | ------------------- |
| `Patient.extension.value`: all 10, including the `valueString` control             | `AnalysisException [FIELD_NOT_FOUND] No such struct field valueSubstanceAmount`         | As expected | Matches this branch |
| `CarePlan.activity.detail.product`: all 7, including the `CodeableConcept` control | `AnalysisException [FIELD_NOT_FOUND] No such struct field productSubstance`             | As expected | Matches this branch |
| `MedicationRequest.medication` and `.reported`: all 4                              | `AnalysisException [INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "true"` | As expected | Matches this branch |
| `Observation.component.value`: the existing non-reference cases                    | As expected                                                                             | As expected | Agree               |

The three failures have different causes, as far as the error messages and a control show:

- **Open choice.** The old list included at least `SubstanceAmount`, which the stored structure of
  `Extension.value` does not carry. Spark reports only the first missing field, so there may have
  been others. So `extension.value.type()` failed whatever the extension held, and the failure is
  not specific to the Reference variant.
- **Closed reference-capable choice.** The old list also held the definition library's per-target
  aliases, of which the error names `productSubstance`, beside `productReference`. No stored
  structure carries such an alias, so `type()` failed for every value of the choice. This is the
  failure the M1 change to `getAllChildTypes` removes, by reducing every target to one
  `<name>Reference` variant.
- **Choice at the resource root.** The old mapper read the variants from the resource's own column,
  not from its table columns. An extra control, run on `v9.9.0` only, gave the same error for a
  non-reference root choice (`Observation.value` holding a string). So this failure has nothing to
  do with references or with M1. Later changes removed it: the mapper now reads a root choice from
  its table columns, and tests each variant's presence through the tolerant reference of the
  parent's representation (FR-026). That form of the mapper arrived in `1ad1951095`. The tolerant
  presence test would also guard against the other two failures, although that was not tested
  separately.

#### Absent values in a plural `type()`: fixed

Over a plural choice, `type()` returned a null entry for each member with no value. An absent value
is not an element of the input, so by the specification it gives nothing. The behaviour predated the
programme: `component.value.type().count()` in `testTypeFunctionOnChoiceFields` pinned 6, and passed
on `v9.9.0` too. A review of this branch found that the new tests had been laid out to avoid it:
the absent member came last, was filtered out, or was counted only through `.name`.

`TypeFunctions.type` now removes the nulls the choice mapper gives, as `applyTo` does for a
traversal. `component.value.type().count()` is now 5. In the two reference-capable tests, the member
with no value now sits between the others, so `type()[1]` is the third member's type only if the
absent one gives no entry. Each test also asserts `type().count()` = 2 and the unfiltered
`type().name` list. fhirpath.js 5.1.0 gives the same answers, because its `typeFn` maps over the
items of the input, and an absent value is not one of them.
- Test runs on this branch: `TypeFunctionsDslTest` 216 run, 0 failed, 2 skipped (the skips are
  existing cases); `ChoiceTypeInfoTest` 2 run, 0 failed; `AbsentElementTest` 117 run, 0 failed. The
  21 new cases all pass. On `v9.9.0`, all 21 new cases fail with the errors above, and the existing
  cases pass.
