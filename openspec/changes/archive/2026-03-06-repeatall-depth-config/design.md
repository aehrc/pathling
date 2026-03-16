## Context

Pathling's `repeatAll()` function uses `UnresolvedTransformTree`, a Catalyst
expression that performs depth-limited recursive tree traversal. The depth
counter only decrements for same-type traversals (where the Spark SQL
`DataType` is identical across levels). Currently, when the depth limit is
exhausted, `UnresolvedTransformTree` silently returns an empty array.

Same-type traversal only occurs when the projection produces an element with an
identical SQL `DataType` at every level. In practice, this means:

- **Extension traversal** (`repeatAll(extension)`): Extension structs have a
  fixed schema at all nesting levels. This is the only legitimate same-type
  recursion — extensions are genuinely recursive in FHIR.
- **Identity-like traversals** (`repeatAll($this)`, `repeatAll(first())`,
  `repeatAll('const')`): These always produce the same type and are effectively
  infinite loops.

The depth limit is currently hardcoded to 10 in
`FilteringAndProjectionFunctions`. The SQL on FHIR `RepeatSelection` has its
own configurable `maxUnboundTraversalDepth` in `QueryConfiguration`, but
`repeatAll()` does not participate in this configuration.

`@FhirPathFunction`-annotated static methods currently receive parameters via
`FunctionParameterResolver`, which supports `Collection`,
`CollectionTransform`, `TypeSpecifier`, and `TerminologyConcepts`. There is no
mechanism to inject the `EvaluationContext` or configuration values into
function methods.

## Goals / Non-Goals

**Goals:**

- Make the same-type recursion depth configurable through a new
  `FhirpathConfiguration` object.
- Rename the depth concept to `maxExtensionDepth` to reflect its primary
  legitimate use case.
- Detect infinite recursive traversal (non-Extension same-type depth
  exhaustion) and raise an evaluation error instead of silently stopping.
- Preserve current silent-stop behavior for Extension traversal.
- Enable `@FhirPathFunction` methods to access the `EvaluationContext`.

**Non-Goals:**

- Changing the SQL on FHIR `RepeatSelection` behavior — it continues to use
  `QueryConfiguration.maxUnboundTraversalDepth` with silent stop semantics.
- Making `UnresolvedTransformTree`'s error message include FHIRPath-level
  context (function name, expression text). A generic error message is
  sufficient.

## Decisions

### Decision 1: Add `errorOnDepthExhaustion` flag to `UnresolvedTransformTree`

Add a boolean `errorOnDepthExhaustion` parameter to `UnresolvedTransformTree`.
When `true` and the same-type depth limit is exhausted, the expression throws
an `AnalysisException` with the message "Infinite recursive traversal
detected." When `false`, the current behavior (returning an empty array) is
preserved.

The flag is propagated through recursive construction in `mapChildren` so all
child nodes inherit the same error mode.

**Alternatives considered:**

- **Detect at FHIRPath level before tree construction**: Cannot know at
  evaluation time whether depth will be exhausted — that depends on schema
  structure resolved during Catalyst analysis.
- **Always error on depth exhaustion**: Would break Extension traversal, which
  legitimately reaches the depth limit.

**Rationale:** The detection naturally lives where the depth is tracked — inside
the Catalyst expression. A simple flag keeps the two modes cleanly separated.

### Decision 2: Thread `errorOnDepthExhaustion` through `ValueFunctions`

`ValueFunctions.transformTree()` and `variantTransformTree()` gain an
`errorOnDepthExhaustion` boolean parameter that is forwarded to
`UnresolvedTransformTree`. Existing callers (`RepeatSelection`) pass `false` to
preserve current behavior.

**Rationale:** Minimal API change — one additional parameter on existing
methods.

### Decision 3: Create `FhirpathConfiguration` record

A new `FhirpathConfiguration` record in the `fhirpath.config` package (or
`fhirpath` package) holds:

```java
public record FhirpathConfiguration(int maxExtensionDepth) {
  public static final int DEFAULT_MAX_EXTENSION_DEPTH = 10;
  public static final FhirpathConfiguration DEFAULT =
      new FhirpathConfiguration(DEFAULT_MAX_EXTENSION_DEPTH);
}
```

**Rationale:** A dedicated configuration object is extensible for future
FHIRPath evaluation settings without changing the `EvaluationContext` interface
each time.

### Decision 4: Add `getConfiguration()` to `EvaluationContext`

The `EvaluationContext` interface gains a default method:

```java
default FhirpathConfiguration getConfiguration() {
  return FhirpathConfiguration.DEFAULT;
}
```

`FhirEvaluationContext` overrides this with the actual configuration.

**Alternatives considered:**

- **Putting configuration on `Collection`**: Would require threading config
  through every collection operation. Too invasive.
- **Static/global configuration**: Not thread-safe, not testable.

**Rationale:** Default method ensures backward compatibility — existing
`EvaluationContext` implementations get sensible defaults without modification.

### Decision 5: Support `EvaluationContext` as a parameter type in `FunctionParameterResolver`

Add a new case in `FunctionParameterResolver.resolveArgument()` that checks for
`EvaluationContext.class.isAssignableFrom(parameter.getType())`. When matched,
the resolver injects the `evaluationContext` directly without consuming an
argument from the argument list.

The `EvaluationContext` parameter is not a FHIRPath argument — it is injected by
the resolver from its own state. This means `bind()` must distinguish between
argument-consuming parameters and context-injected parameters when iterating
method parameters.

**Rationale:** General-purpose mechanism — other functions may need context
access in the future. Consistent with how the resolver already handles different
parameter types.

### Decision 6: Extension detection in `Collection.repeatAll()`

After applying the transform to get `resultTemplate`, check:

```java
boolean isExtensionTraversal = resultTemplate.getFhirType()
    .map(FHIRDefinedType.EXTENSION::equals)
    .orElse(false);
```

Pass `errorOnDepthExhaustion = !isExtensionTraversal` to
`variantTransformTree()`.

**Rationale:** The `fhirType` metadata is already available on the result
collection. Extension is the only FHIR type with a fixed, non-truncating schema
that produces legitimate same-type recursion.

## Risks / Trade-offs

**Error during Catalyst analysis** → The error is thrown during query planning,
not data execution. This means `repeatAll($this)` always errors regardless of
data content. This is correct — infinite recursion is a static property of the
expression, not a runtime condition.

**Unknown fhirType defaults to error mode** → When `getFhirType()` returns
empty, the traversal defaults to error mode (`!false = true`). Types with no
FHIR type mapping (e.g., custom structures) that happen to have same-type
recursion will error. This is conservative and correct — unknown types that hit
the depth limit are more likely bugs than intentional recursion.

**`RepeatSelection` unaffected** → The SQL on FHIR `RepeatSelection` passes
`false` for `errorOnDepthExhaustion`, preserving its current silent-stop
behavior. This is intentional — `RepeatSelection` serves a different use case
where depth limiting is a normal operating mode, not an error condition.
