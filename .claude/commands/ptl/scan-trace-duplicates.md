---
name: "PTL: Scan Trace Duplicates"
description: "Scan Java source packages for Spark SQL expressions that can cause duplicate TraceExpression evaluations (issue #2594 class of bug). Invoked as /ptl:scan-trace-duplicates [package-paths...]. Defaults to au.csiro.pathling.fhirpath and au.csiro.pathling.sql in the fhirpath module."
category: Quality
tags: [pathling, spark, trace, fhirpath, quality]
---

Scan the specified Java packages for Spark SQL expression patterns that trigger multiple
evaluations of `Nondeterministic` expressions (such as `TraceExpression`), causing duplicate
`trace()` log entries per row (GitHub issue #2594).

## Arguments

`$ARGUMENTS` — an optional space-separated list of Java package paths to scan, e.g.:

```
au.csiro.pathling.fhirpath.column au.csiro.pathling.sql
```

If omitted, default to:
- `au.csiro.pathling.fhirpath`
- `au.csiro.pathling.sql`

in the `fhirpath` module.

---

## Background

`TraceExpression` is a Catalyst `Nondeterministic` expression. Spark's Common Subexpression
Elimination (CSE) **does not** deduplicate nondeterministic nodes — every reference to the same
`Column` variable in the assembled Catalyst plan fires the expression independently. This means
that if a method receives a `Column` parameter and references it N times in a single Spark SQL
expression, a traced operand will fire N times per row instead of once.

`ColumnHelpers.let(value, body)` is the fix: it materialises a potentially nondeterministic column
exactly once using `element_at(transform(array(value), body::apply), 1)`. For deterministic columns
it inlines directly with no overhead. Lambda params inside `let()` are deterministic and safe to
reference multiple times.

### Examples of bugs fixed in PR #2594 (use these as recognition patterns)

| File | Bug pattern | Fix |
|------|-------------|-----|
| `ColumnRepresentation.toArray()` | `when(c.isNotNull(), array(c))` — `c` referenced twice | `let(c, x -> when(x.isNotNull(), array(x)))` |
| `ColumnRepresentation.filter()` | `when(c.isNotNull(), when(lambda.apply(c), c))` — `c` referenced 3× | `let(c, x -> when(x.isNotNull().and(lambda.apply(x)), x))` |
| `ColumnRepresentation.normaliseNull()` | `when(c.isNull().or(size(c).equalTo(0)), null).otherwise(c)` — `c` 3× | `let(c, x -> when(size(x).equalTo(0), lit(null)).otherwise(x))` |
| `ColumnRepresentation.transform()` | `when(c.isNotNull(), lambda.apply(c))` — `c` 2× | `let(c, x -> when(x.isNotNull(), lambda.apply(x)))` |
| `ColumnRepresentation.singular()` | `when(c.isNull().or(size(c).leq(1)), getAt(c,0))` — `c` 3× | `let(c, x -> when(size(x).gt(1), raise_error(...)).otherwise(getAt(x,0)))` |
| `ConversionLogic.convertToBoolean` | `when(value.equalTo("1.0"), ...).otherwise(value.try_cast(...))` — `value` 3× | `let(value, v -> when(...).otherwise(v.try_cast(...)))` |
| `ConversionLogic.convertToDate/DateTime/Time` | `when(value.rlike(REGEX), value)` — `value` 2× | `let(value, v -> when(v.rlike(REGEX), v))` |
| `QuantityValue.toUnit()` / `convertibleToUnit()` | `quantityColumn` referenced 5× in assembled expression | `let(quantityColumn, qc -> ...)` |
| `CodingEquality.equalsTo()` | `left` and `right` each referenced multiple times | `let(left, l -> let(right, r -> ...))` |
| `ColumnRepresentation.containsElement()` | `element.getValue()` referenced twice | `let(element.getValue(), ev -> ...)` |

---

## Scan Procedure

### Step 1 — Resolve file paths

Parse `$ARGUMENTS` as a space-separated list of Java package names. Convert each to a directory
path by replacing `.` with `/` and prefixing with the module source root:

```
fhirpath/src/main/java/<package/path>/
```

If `$ARGUMENTS` is empty, use:
```
fhirpath/src/main/java/au/csiro/pathling/fhirpath/
fhirpath/src/main/java/au/csiro/pathling/sql/
```

Enumerate all `.java` files recursively:
```bash
find fhirpath/src/main/java/au/csiro/pathling/fhirpath \
     fhirpath/src/main/java/au/csiro/pathling/sql \
     -name "*.java" | sort
```

### Step 2 — Partition and dispatch agents

Partition the file list into groups of **8–10 files** each. Launch one Haiku subagent per group
**in a single parallel turn**. Give each agent this instruction:

---

> Read each of the following Java files and identify any method that receives or holds a `Column`
> (or `ColumnRepresentation`) and references the same variable more than once within a single
> assembled Spark SQL expression tree.
>
> Look specifically for these combinator patterns where the same variable appears in multiple
> positions:
> - `when(x.isNotNull(), ...).otherwise(x)` — predicate + value branch
> - `when(x.rlike(...), x)` — predicate + value branch
> - `size(x)` plus `x` in the same expression
> - `when(x.equalTo(...), ...).when(x.equalTo(...), ...).otherwise(x.tryCast(...))` — multiple branches
> - `callUDF(..., x, ...)` combined with `x.getField(...)` or similar
> - `x.isNull().or(rightColumn.isNull())` plus `x.getField(...)` in the same expression
> - `coalesce(x, ...)` where `x` also appears elsewhere in the expression
> - `exists(arr, e -> comparator.apply(e, x))` where `x` is also used in the predicate
>
> For each site found, report:
> - File path and method name
> - Which variable is referenced multiple times and how many times
> - The specific expression pattern (brief code quote)
> - Whether any of the references are inside a `let()` lambda parameter (those are safe — lambda
>   params like `qc`, `lv`, `rv`, `x`, `v` etc. are deterministic)
> - Your assessment: **GENUINE BUG**, **LATENT RISK**, or **FALSE POSITIVE** (see triage rules below)
>
> **Triage rules:**
> - **GENUINE BUG**: Variable referenced multiple times AND the method can be called with a
>   nondeterministic column (e.g., any `Column` parameter, any `this.column` field populated from
>   an arbitrary caller).
> - **LATENT RISK**: Multiple references but the method is only ever called with columns that are
>   structurally deterministic at all current call sites. Document; suggest adding a Javadoc note.
> - **FALSE POSITIVE**: The variable is a `let()` lambda parameter, or the expression tree only
>   evaluates it once despite appearing multiple times in the Java source (e.g., builder-style APIs
>   where intermediate `Column` objects are not re-evaluated).
>
> Files to scan:
> [LIST OF FILE PATHS]

---

### Step 3 — Aggregate and triage results

Collect all agent reports. Produce a single summary with three sections:

#### GENUINE BUGS

For each genuine bug:
1. State file path, method, variable, and reference count.
2. Show the buggy expression (brief snippet).
3. Recommend the fix:
   ```java
   return let(myColumn, mc -> {
       // use mc everywhere instead of myColumn
   });
   ```
4. Suggest a regression test following the pattern in
   `QuantityValueTraceTest.java` or `TraceFunctionTest.java`: wrap the input column with
   `TraceExpression`, evaluate on a single-row dataset, assert exactly 1 trace log entry via
   Logback `ListAppender`.

#### LATENT RISKS

For each latent risk: state file, method, variable, why it is not currently triggered, and
recommend a Javadoc note like:
```java
// NOTE: callers must not pass nondeterministic columns; wrap with let() if needed.
```

#### FALSE POSITIVES

List briefly with justification (lambda param, builder API, etc.).

---

## Key Reference Files

| File | Purpose |
|------|---------|
| `fhirpath/src/main/java/au/csiro/pathling/sql/SqlFunctions.java` | `let()` implementation |
| `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/ColumnHelpers.java` | `let()` helper (if present) |
| `fhirpath/src/main/java/au/csiro/pathling/fhirpath/column/QuantityValue.java` | Fixed example (`toUnit`, `convertibleToUnit`) |
| `fhirpath/src/test/java/au/csiro/pathling/fhirpath/column/QuantityValueTraceTest.java` | Layer-B test pattern |
| `fhirpath/src/test/java/au/csiro/pathling/fhirpath/column/ColumnRepresentationTraceTest.java` | Layer-B test pattern |
| `fhirpath/src/test/java/au/csiro/pathling/fhirpath/function/provider/TraceFunctionTest.java` | End-to-end trace count pattern |
| `config/checkstyle/checkstyle.xml` | `RepeatedSqlEvaluation` Checkstyle rule (catches simple `when`/`otherwise` cases) |

The Checkstyle `RepeatedSqlEvaluation` rule catches `when(ID..., ID)` and `when(ID...).otherwise(ID)`
where the same ≥7-char identifier appears in both branches. Lambda params in `let()` are ≤6 chars
and never trigger this rule. The rule catches simple cases but misses multi-reference patterns
outside `when()/otherwise()` — this scan covers those gaps.

---

## Safe Patterns (do not flag)

- References to `let()` lambda parameters (e.g., `qc`, `lv`, `rv`, `x`, `v`, `nc`, `ev`)
- Columns derived from a `let()` lambda param (e.g., `new QuantityValue(qc).isUcum()` where `qc`
  is a lambda param)
- Builder-style APIs where each intermediate column value is a new node (not a shared reference)
- `lit(...)`, `col(...)`, and other factory calls that create new expressions each time
