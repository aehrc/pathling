## Context

The FHIRPath `repeat()` / `repeatAll()` functions and the ViewDefinition
`repeat` clause both perform recursive traversal with depth limiting, but
they are configured independently:

- **FHIRPath functions** read `maxUnboundTraversalDepth` from
  `FhirpathConfiguration`, accessed via `EvaluationContext`. When no explicit
  configuration is provided, the default (10) is used. The library API's
  `SingleResourceEvaluator` always uses the 4-arg `FhirEvaluationContext`
  constructor, which falls back to `FhirpathConfiguration.DEFAULT` — ignoring
  any user-configured depth.

- **ViewDefinition repeat clause** reads `maxUnboundTraversalDepth` from
  `QueryConfiguration`, passed to `RepeatSelection` via `FhirViewExecutor`.
  This path respects user configuration but silently truncates on depth
  exhaustion for all types.

The `@Min` validation constraints also differ: `FhirpathConfiguration` uses
`@Min(1)` while `QueryConfiguration` uses `@Min(0)`.

## Goals / Non-Goals

**Goals:**

- Align `@Min` constraints so both config classes reject zero as a depth value.
- Wire `QueryConfiguration.maxUnboundTraversalDepth` through to
  `FhirpathConfiguration` so FHIRPath `repeat()` / `repeatAll()` respect user
  configuration in the library API.
- Make `RepeatSelection` error at runtime when non-Extension traversals exhaust
  the depth limit, matching `repeatAll()` behavior.

**Non-Goals:**

- Merging `QueryConfiguration` and `FhirpathConfiguration` into a single class.
  They operate at different levels and carry different concerns.
- Adding static type analysis to `RepeatSelection`. Runtime depth exhaustion
  errors are sufficient; the statically-detectable infinite recursion cases in
  `Collection.repeatAll()` exist because primitives cannot be converted to
  Variants, which is not a concern for `RepeatSelection`.

## Decisions

### 1. Align `@Min` constraint on `QueryConfiguration`

Change `QueryConfiguration.maxUnboundTraversalDepth` from `@Min(0)` to
`@Min(1)`. A depth of 0 is meaningless (no recursion at all) and inconsistent
with `FhirpathConfiguration`.

**Alternative**: Keep `@Min(0)` and interpret 0 as "disable recursion". Rejected
because it adds complexity for no practical benefit and diverges from the
FHIRPath config.

### 2. Add `FhirpathConfiguration` to `SingleResourceEvaluator`

Add a `FhirpathConfiguration` field to `SingleResourceEvaluator` and pass it
through to `FhirEvaluationContext` in the `evaluate()` method. Add a
corresponding `withConfiguration()` method to `SingleResourceEvaluatorBuilder`.

The `SingleResourceEvaluator.of()` factory method retains the current 3-arg
signature with default configuration, preserving backward compatibility for
callers that don't need custom configuration.

**Alternative**: Pass `FhirpathConfiguration` directly to the `evaluate()`
method. Rejected because configuration is a property of the evaluator, not a
per-evaluation parameter.

### 3. Map `QueryConfiguration` to `FhirpathConfiguration` at API boundaries

At each site where `QueryConfiguration` is available and a
`SingleResourceEvaluator` is constructed, build a `FhirpathConfiguration` from
the query config's `maxUnboundTraversalDepth` and pass it to the evaluator
builder.

Affected sites:

- `FhirViewExecutor` — already has `QueryConfiguration`, needs to pass it to
  the evaluator builder.
- Library API sources (`AbstractSource`, `QueryContext`) — already have access
  to `PathlingContext.getQueryConfiguration()`.

### 4. `RepeatSelection` uses `errorOnDepthExhaustion` flag

`ValueFunctions.transformTree()` already has a 5-arg overload with
`errorOnDepthExhaustion`. `RepeatSelection` currently calls the 4-arg overload
(which defaults to `false`).

Change `RepeatSelection` to:

1. Determine the FHIR type of each traversal path result from the starting
   node's `Collection`.
2. Pass `errorOnDepthExhaustion = true` for non-Extension types, `false` for
   Extension types.

Since `RepeatSelection` already has access to `Collection` objects via
`context.evalExpression(path)`, it can check `getFhirType()` against
`FHIRDefinedType.EXTENSION`.

### 5. `RepeatSelection` receives `maxDepth` from `QueryConfiguration`

No change needed here — `RepeatSelection` already receives `maxDepth` from
`FhirViewExecutor`, which reads it from `QueryConfiguration`. This path is
already correctly wired.

## Risks / Trade-offs

- **`@Min(0)` → `@Min(1)` is a breaking change** for any caller that
  explicitly sets `maxUnboundTraversalDepth` to 0. → Mitigation: This is
  unlikely in practice since depth 0 produces no useful results. Document in
  release notes.

- **Runtime errors from `RepeatSelection` depth exhaustion** could surface for
  ViewDefinitions that previously worked silently. → Mitigation: Only
  non-Extension same-type depth exhaustion triggers the error. Users can
  increase `maxUnboundTraversalDepth` to resolve.
