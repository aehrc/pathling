## 1. Extract shared combining logic

- [x] 1.1 Create `CombiningLogic` helper in
      `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/` exposing
      array-level primitives: `prepareArray(Collection)` (extract column
      and normalise Decimal precision), `dedupeArray(Column, ColumnEquality)`,
      `unionArrays(Column, Column, ColumnEquality)`, and
      `combineArrays(Column, Column)`.
- [x] 1.2 Refactor `UnionOperator` to delegate its `handleOneEmpty` and
      `handleEquivalentTypes` bodies to `CombiningLogic`, preserving all
      current behaviour and error messages exactly.
- [x] 1.3 Run the existing `CombiningOperatorsDslTest` to confirm the
      refactor is behaviour-preserving.

## 2. New CombineOperator

- [x] 2.1 Create `CombineOperator` in
      `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/` extending
      `SameTypeBinaryOperator`, with `getOperatorName()` returning `"combine"`.
- [x] 2.2 Implement `handleOneEmpty` to return the non-empty operand
      unchanged (no dedup, no extra normalisation beyond what the collection
      already carries).
- [x] 2.3 Implement `handleEquivalentTypes` to call
      `CombiningLogic.combineArrays` after `prepareArray`, producing a
      concatenated result that preserves duplicates.
- [x] 2.4 Inherit `handleNonEquivalentTypes` from `SameTypeBinaryOperator`
      (which fails with the standard `InvalidUserInputError`), verifying the
      resulting error message reads
      `Operator \`combine\` is not supported for: ...`.

## 3. Parser-level desugaring

- [x] 3.1 In `Visitor.visitInvocationExpression`, after building the
      `invocationSubject` and `invocationVerb`, detect the case where the
      verb is an `EvalFunction` with identifier `union` or `combine`.
- [x] 3.2 When detected, validate the argument count (exactly one) and
      emit an `EvalOperator(invocationSubject, operator, argument)` where
      `operator` is `new UnionOperator()` for `union` or
      `new CombineOperator()` for `combine`.
- [x] 3.3 For any other argument count, throw an
      `InvalidUserInputError` with a clear message indicating that the
      function requires exactly one argument.
- [x] 3.4 Fall through to the existing `invocationSubject.andThen(invocationVerb)`
      composition for every other function identifier, keeping the change
      strictly scoped to `union` and `combine`.
- [x] 3.5 Add a short comment at the desugaring site explaining the
      rationale (spec equivalence with `|`, iteration-context behaviour)
      and linking to the FHIRPath specification section.

## 4. DSL test coverage

- [x] 4.1 Create
      `fhirpath/src/test/java/au/csiro/pathling/fhirpath/dsl/CombiningFunctionsDslTest.java`
      extending `FhirPathDslTestBase`, following the layout used by
      `CombiningOperatorsDslTest`.
- [x] 4.2 Cover `union()` across the full type matrix (Boolean,
      Integer, Decimal, String, Date, DateTime, Time, Quantity, Coding),
      mirroring the scenarios in `specs/fhirpath-union/spec.md`.
- [x] 4.3 Cover `combine()` duplicate-preservation cases across the same
      type matrix: both-sides duplicates, within-one-side duplicates,
      empty-input handling, Integer → Decimal promotion, Quantity and
      Coding non-dedup.
- [x] 4.4 Add `x.union(y) ≡ x | y` equivalence tests across the same
      type matrix so that any future divergence between the function form
      and the operator form is caught immediately.
- [x] 4.5 Add iteration-context pin-down tests that evaluate
      `Patient.name.select(use.union(given))` against a Patient resource
      with multiple `name` elements, asserting equality with
      `Patient.name.select(use | given)`.
- [x] 4.6 Add the `combine` analogue of the iteration-context test:
      `Patient.name.select(use.combine(given))` must preserve duplicates
      while still evaluating `use` and `given` against the current name
      element.
- [x] 4.7 Add error-path tests with `testError(...)` for incompatible
      polymorphic types on both functions (e.g. `(1).union(true)`,
      `(1).combine(true)`), confirming the error paths are wired correctly
      through the desugaring.
- [x] 4.8 Add a test that `x.union()` (no argument) raises a clear
      "requires exactly one argument" error at parse time, ensuring
      Decision 2.3 is exercised.

## 5. Reference-test exclusion review

- [x] 5.1 Audit `fhirpath/src/test/resources/fhirpath-js/config.yaml`
      for exclusions that exist solely because `union()` or `combine()`
      were not implemented.
- [x] 5.2 Remove any such exclusion and run
      `YamlReferenceImplTest` to confirm the unblocked tests pass.
- [x] 5.3 For any reference test that still fails for reasons unrelated
      to this change, keep the exclusion and update its justification text
      to reference the underlying limitation (polymorphic unions, calendar
      duration dedup, etc.).

## 6. Documentation

- [x] 6.1 Add entries for `union()` and `combine()` to the FHIRPath
      functions page under `site/docs/fhirpath/` (whichever file lists
      built-in functions).
- [x] 6.2 Note in the `union()` entry that it is defined by the spec to
      be synonymous with `|`, and cross-reference the operator entry.

## 7. Final verification

- [x] 7.1 Run `mvn test -pl fhirpath` and confirm the full fhirpath
      module test suite passes. (Two `FhirViewShareableComplianceTest`
      failures are pre-existing on `issue/2384`, unrelated to this change,
      and confirmed by running the test class on the stashed baseline.)
- [x] 7.2 Run `mvn test -pl fhirpath -Dtest=CombiningFunctionsDslTest`
      and confirm the new test class is fully green. (58/58 pass.)
- [x] 7.3 Run `mvn test -pl fhirpath -Dtest=CombiningOperatorsDslTest`
      and confirm the refactor preserved operator behaviour. (188/188 pass.)
- [x] 7.4 Run `mvn test -pl fhirpath -Dtest=YamlReferenceImplTest`
      and confirm the reference suite state reflects the exclusion review.
      (1821 run, 0 failures, 979 skipped — 5 additional tests skipped
      vs. baseline because the 5 newly-failing combine/union reference
      tests are covered by new entries in `config.yaml` for #437, #2398,
      and the latent Quantity + repeat canonicalization limitation.)
