## Why

Pathling implements the `|` union operator but does not implement the FHIRPath
[combining functions](https://hl7.org/fhirpath/#combining) `union()` and
`combine()`. The spec declares `x.union(y)` to be _synonymous_ with `x | y`, so
users writing expressions in the function form — which is common in
specifications and tutorials — currently get a "function not found" error even
though the underlying capability already exists. `combine()` (a union that
preserves duplicates) has no equivalent at all and is simply missing.

Closing this gap unblocks idiomatic FHIRPath authoring, improves conformance
against the reference fhirpath.js test suite, and adds one genuinely new
capability (`combine()`) that cannot currently be expressed.

## What Changes

- Add the FHIRPath `union(other : collection) : collection` function that
  merges two collections and eliminates duplicates, with behaviour strictly
  equivalent to the existing `|` operator.
- Add the FHIRPath `combine(other : collection) : collection` function that
  merges two collections without eliminating duplicates.
- Implement both functions via **parser-level desugaring**: the invocation
  visitor rewrites `x.union(y)` and `x.combine(y)` into the same
  `EvalOperator` AST shape that `x | y` uses, so semantic equivalence with
  the operator form — including the spec's
  `name.select(use.union(given)) ≡ name.select(use | given)` equivalence
  inside iteration contexts — is guaranteed by construction.
- Introduce a new `CombineOperator` (peer to `UnionOperator`) that provides
  the concatenate-without-dedup merge step used by the desugared
  `combine()` form.
- Extract a shared `CombiningLogic` helper so that the existing
  `UnionOperator` and the new `CombineOperator` share type reconciliation,
  Decimal normalization, and comparator-aware merging.
- Neither `union` nor `combine` is registered in the function registry —
  the FHIRPath spec defines them only as method invocations on an input
  collection, and the desugaring happens before the registry is consulted.
- Remove any corresponding exclusions from
  `fhirpath/src/test/resources/fhirpath-js/config.yaml` that exist solely
  because these functions were not implemented.

No behaviour of the existing `|` operator changes. There are no breaking
changes.

## Capabilities

### New Capabilities

- `fhirpath-union`: The FHIRPath `union(other)` function that merges two
  collections and eliminates duplicates via equality, synonymous with `|`.
- `fhirpath-combine`: The FHIRPath `combine(other)` function that merges two
  collections without eliminating duplicates.

### Modified Capabilities

<!-- None - no existing spec-level requirements are changing. -->

## Impact

- **Code**:
    - `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/CombiningLogic.java` — new shared helper holding the array-level merge primitives.
    - `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/CombineOperator.java` — new same-type binary operator for the concatenate-without-dedup merge.
    - `fhirpath/src/main/java/au/csiro/pathling/fhirpath/operator/UnionOperator.java` — refactor to delegate its merge steps to `CombiningLogic`.
    - `fhirpath/src/main/java/au/csiro/pathling/fhirpath/parser/Visitor.java` — desugar `x.union(y)` / `x.combine(y)` in the invocation visitor into the corresponding `EvalOperator` AST.
- **Tests**:
    - New DSL test file `CombiningFunctionsDslTest` covering `union()`,
      `combine()`, a `union() ≡ |` equivalence property, and iteration-context
      pin-down cases inside `select`.
    - Possible removal of YAML exclusions in
      `fhirpath/src/test/resources/fhirpath-js/config.yaml` that become
      redundant once the functions exist.
- **APIs**: The public FHIRPath surface gains two new function forms that
  share behaviour with existing/new binary operators. The Java, Python, and
  R library APIs are unaffected beyond what the FHIRPath engine already
  exposes.
- **Dependencies**: None.
- **Documentation**: `site/docs/fhirpath/functions.md` (or equivalent) should
  gain entries for `union()` and `combine()`.
