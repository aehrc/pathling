## Context

The `trace()` function is implemented as a unary Spark Catalyst expression
(`TraceExpression`) that logs and returns its single child. The FHIRPath spec
defines an optional second argument (`projection: Expression`) that allows
logging a derived value while still returning the original input. The function
argument infrastructure (`CollectionTransform`) already supports expression-type
parameters, used by `where()`, `select()`, `exists()`, etc.

## Goals / Non-Goals

**Goals:**

- Support the optional projection argument: `trace(name, projection)`
- Ensure the logged value and FHIR type reflect the projection result when
  provided
- Fix existing issue: mark `TraceExpression` as `Nondeterministic` to prevent
  Catalyst from optimizing away side effects

**Non-Goals:**

- Dynamic name expressions (name must remain a string literal)
- Changes to `TraceCollector` interface or `ListTraceCollector`
- Changes to library-api, Python, or R bindings

## Decisions

### 1. Single binary expression (always binary)

Convert `TraceExpression` from `UnaryExpression` to `BinaryExpression`. The
first child (`left`) is the pass-through value; the second child (`right`) is
the value to log.

When no projection is provided, both children reference the same column. When a
projection is provided, `right` is the projected column.

**Alternatives considered:**

- **Two separate expressions (unary + binary)**: Avoids double-evaluation of the
  same column when no projection is provided, but duplicates logging/formatting
  logic across two classes.
- **Single expression with `Option[Expression]`**: Cannot extend
  `UnaryExpression` or `BinaryExpression`; requires manual `children` and
  `withNewChildren` management.

**Rationale:** Minimal code duplication. The shared logging, formatting, and
collector logic stays in one place. Double-evaluation of the same column in the
no-projection case is negligible for a diagnostic function.

### 2. Mark TraceExpression as Nondeterministic

`TraceExpression` has side effects (SLF4J logging, collector accumulation).
Without the `Nondeterministic` trait, Catalyst could theoretically eliminate
duplicate trace expressions via common subexpression elimination.

### 3. Custom eval() instead of nullSafeEval for binary form

`BinaryExpression.nullSafeEval` skips evaluation if either child is null. We
need asymmetric null handling:

- If the pass-through value (left) is null, return null (no logging).
- If the projected value (right) is null, skip logging but still return the
  pass-through value.

Override `eval()` directly to implement this.

### 4. Projection evaluated at FHIRPath level

The `trace()` function evaluates the `CollectionTransform` projection on the
input collection at the FHIRPath level, extracting the resulting column and FHIR
type. These are passed to `TraceExpression` as constructor arguments. The
Catalyst expression does not know about FHIRPath.

## Risks / Trade-offs

- **Double evaluation when no projection**: When both children are the same
  column expression, Spark evaluates it twice per row. Mitigated by the fact
  that trace is a diagnostic tool, not performance-critical. Can refactor to two
  expression classes later if profiling shows impact.
- **Nondeterministic disables CSE**: Marking the expression nondeterministic
  prevents Catalyst from caching results across duplicate trace expressions.
  This is the correct behavior (each trace should fire its side effects) but
  worth noting.
