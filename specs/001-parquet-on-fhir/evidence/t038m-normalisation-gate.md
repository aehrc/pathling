# The normalisation gate

T038m is the gate on M2's strategy. Decision 55 moves layout dispatch out of the
collection classes and into the traversal expression. The expression would
normalise the previous layout to the new one, so the engine above it sees one
layout. The case in doubt was extensions. The previous layout keys a root
extension map, `_extension`, by each element's `_fid`, and the expression has to
reach that map.

The task asks three questions, in order:

1. Can the expression derive the map from its own child, by walking the child's
   extraction chain to the resource column?
2. Inside a `transform` lambda, where the walk stops at the lambda variable, does
   a binary form survive the analyzer on T009a's terms, with the map supplied as
   a second child?
3. Is per-level normalisation enough for the self-recursive extension type?

**Result: the gate fails on question 1.**

| Question | Verdict |
| --- | --- |
| 1. Unary self-derivation of the root map | **FAIL** |
| 2. Binary form, with the map supplied from the retained handle | **PASS** |
| 3. Per-level normalisation for `extension.extension` | **PASS**, with a type caveat |
| Overall, under T038m's rule that a failure of question 1 or 2 fails the gate | **FAIL** |

Question 1 does not fail because of Catalyst. It fails because its premise is
false for the engine as it stands. Decisions 48 and 55 both say the map "is a
field on the resource struct". But the engine has no resource struct. The binary
form works at every site the engine produces, and gives the same rows as the
unmodified engine. So this evidence supports amending decision 55 to use the
binary form everywhere, as much as it supports reverting to decision 47. The
owner makes that choice.

## Conditions

| | |
| --- | --- |
| Commit | `18e59220ec` (`issue/2762`) |
| Machine | Apple M3 Pro, 11 cores, 36 GB, macOS 26.6.2 |
| JVM | Java HotSpot 21.0.3+7-LTS-152 |
| Spark | 4.0.2 (Scala 2.13) |
| Session | The `fhirpath` test session (`@SpringBootUnitTest`), ANSI mode on |
| Data | Three Patients encoded by `FhirEncoders` in the previous layout, written to Parquet and read back so that the optimiser does not fold the plan into a local relation |

The spike code is not in the repository. It was run and then deleted:

- the expressions, in `encoders/src/main/scala`, because `fhirpath` has no Scala
  compilation;
- a temporary hook in `Collection.traverseExtension`;
- a driver test in `fhirpath/src/test`.

`encoders` was reinstalled afterwards, so `~/.m2` holds no spike classes. The
transcript is `t038m-normalisation-gate.txt`, beside this file. Its very long
lines are elided in the middle. The elided text is the previous layout's
`Extension` struct type, repeated.

## What was built

Both expressions follow the construction that T009a proved:

- `RuntimeReplaceable` with `UnaryLike` or `BinaryLike`, not
  `InheritAnalysisRules`;
- `replacement` is a `lazy val` on a case class;
- the replacement is built only from resolved nodes: `GetStructField`,
  `GetMapValue`, `Flatten`, `ArrayFilter`, `ArrayTransform`, `IsNotNull`, and a
  `LambdaFunction` over a fresh `NamedLambdaVariable`, which is resolved because
  its type is given.

```scala
// Question 1: derive the map by walking the child.
case class SpikeExtNormUnary(child: Expression)
  extends RuntimeReplaceable with UnaryLike[Expression] {
  override lazy val replacement: Expression = walk(child) match {
    case Some(resourceStruct) => lookup(child, GetStructField(resourceStruct, <_extension>))
    case None => throw ... // Logged together with the walk trace and the leaves reached.
  }
}

// Question 2: the map is supplied as a second child.
case class SpikeExtNormBinary(left: Expression, right: Expression)
  extends RuntimeReplaceable with BinaryLike[Expression] {
  override lazy val replacement: Expression = lookup(left, right)
}

// Shared: an element's extensions, looked up in the root map by its _fid.
//   struct with _fid        -> map[child._fid]
//   array of struct w/ _fid -> flatten(filter(transform(child, x -> map[x._fid]), y -> y is not null))
//   int (the resource _fid) -> map[child]
```

The walk visits every node under the child. It records each node's class and
type, collects the leaves, and looks for an `AttributeReference` whose type is a
struct carrying `_extension`.

**The engine produced every plan.** A temporary hook in
`Collection.traverseExtension` emitted the spike expression in place of the
existing map lookup, at every extension step. That includes the steps inside
lambda bodies and at every level of recursion. The hook passed:

- **Unary mode:** the parent collection's column value as the child.
- **Binary mode:** the same child, and `extensionMapColumn` as the map. That is
  the resource-level handle the engine already carries, and it stands in for the
  handle T094 retains.

At the resource root there is no element column to extract `_fid` from, so the
binary hook passed the resource's own `_fid` column. That column is a top-level
sibling, taken from the same resource handle.

## How it was measured

This follows T009a:

- The expression logs every forcing of `replacement`, with the child's class and
  its `resolved` flag at that moment.
- The unary form also logs the full walk trace and the leaves it reached.
- Each binary run captures the plan the engine built, the analyzed plan, the
  optimised plan, the result schema and the rows.

**The positive control**: every expression was run three times:

1. unmodified (hook off);
2. unary;
3. binary.

The binary rows were then compared with the unmodified engine's rows.

## Scenarios

| Scenario | Expression | Unary | Binary rows equal unmodified |
| --- | --- | --- | --- |
| Resource root | `extension.url` | FAIL: child is `Literal(true)` | yes |
| Repeating parent | `name.extension.url` | FAIL: walk ends at `name#87` | yes |
| Filtered parent (the task's lambda case) | `name.where(use='official').extension.url` | FAIL: walk ends at `name#87` | yes |
| Lambda child, `where` | `name.where(extension('urn:name1').exists()).family` | FAIL: child is `NamedLambdaVariable` | yes |
| Lambda child, `select` | `name.select(extension.url)` | FAIL: child is `NamedLambdaVariable` | yes |
| Recursive | `extension.extension.url` | FAIL at level 1 | yes |
| Recursive, depth 3 | `extension.extension.extension.url` | FAIL at level 1 | yes |
| Recursive, through `extension()` | `extension('urn:ex3').extension('urn:ex3_1').value.ofType(string)` | FAIL at level 1 | yes |
| Recursive, lambda, `select` | `extension.select(extension.url)` | FAIL at level 1 | yes (see below) |
| Recursive, lambda, `where` | `extension.where(extension.exists()).url` | FAIL at level 1 | yes |
| Element type, level 1 | `extension` | FAIL | yes |
| Element type, level 2 | `extension.extension` | FAIL | yes |
| *Control, hand-built:* unary over a `struct(*)` root | `P.name` → extensions → `url` | resolves, correct urls | n/a |
| *Control:* binary over data without `_extension` | `name.extension.url` | n/a | `UNRESOLVED_COLUMN` |

## Question 1: FAIL

The engine does not represent a resource as a struct column. Both resolvers
build the resource from `ResourceRepresentation.alwaysPresent()`:

- Its value is `lit(true)`.
- Its traversal emits a top-level column, `CASE WHEN isnotnull(true) THEN <field> END`.

`_extension` is therefore a top-level attribute that sits beside `name`. It is
not a field of anything the child contains. `ResourceCollection`'s
`columnRepresentation.traverse(EXTENSIONS_FIELD_NAME)`, which the premise
checks relied on, is `col("_extension")` on this representation, not a
struct-field access.

The walk shows this directly. For `name.extension`:

    [unary] walk from child:
        ArrayFilter: array<struct<...,_fid:int>>
          CaseWhen: array<struct<...,_fid:int>>
            IsNotNull: boolean
              Literal(true): boolean
            AttributeReference(name#87): array<struct<...,_fid:int>>
          LambdaFunction: boolean
            ...
    [unary] NO resource struct carrying _extension under the child: walk FAILED

At the resource root the child is just `Literal(true)`, because
`ResourceRepresentation.getValue()` is the existence column. Every recursive
case fails at level 1 for that reason. Inside a lambda, the only leaf is the
`NamedLambdaVariable`, as the task anticipated.

A replacement cannot conjure `_extension#104`:

- An unresolved reference is rejected by `CheckAnalysis`, as T009a found.
- A resolved `AttributeReference` needs the attribute's `exprId`, which the
  expression cannot see.

**The control shows that the construction is not at fault.** Over a
hand-built `struct(*)` root, the same unary expression finds the struct and
resolves. In this one `Project`-shaped query, the optimiser also collapsed the
struct construction to the bare column, so the plan reads
`_extension#104[lambda x._fid]` directly:

    Project [id#79, transform(flatten(filter(transform(name#87,
      lambdafunction(_extension#104[lambda x#479._fid], ...)), ...)), ...) AS value#478]

A struct root would therefore rescue the unary form. But the engine does not
produce one, and introducing one is a change to `ResourceRepresentation`, which
is outside T038m.

## Question 2: PASS

The binary form survived the analyzer in all twelve engine scenarios. Every
forcing saw both children resolved, with no throw and no premature probe.

The analyzed plan keeps the node with both children resolved. Inside a lambda,
the left child is the lambda variable and the right child is an outer
reference to the top-level map:

    spikeextnormbinary(lambda x_58#224, CASE WHEN isnotnull(true) THEN _extension#104 END)

The optimised plan contains the replacement. The outer reference to the map
sits correctly inside the lambda body:

    filter(name#87, lambdafunction(... _extension#104[lambda x_58#224._fid] ..., lambda x_58#224, false))

In the repeating-parent case the replacement is the array form:

    flatten(filter(transform(filter(name#87, ...),
      lambdafunction(_extension#104[lambda x#150._fid], lambda x#150, false)), ...))

At the resource root it is `_extension#104[_fid#103]`. In every scenario the
binary rows equal the unmodified engine's rows.

**The binary form is needed at every site, not only inside lambdas.** The task
expected the unary walk to work outside a lambda and fail inside one. On this
engine it works nowhere. At the resource root the element's `_fid` also has to
come from the handle, because there is no element column to extract it from.

**A plain handle does not tolerate the new layout.** Over data without
`_extension`, which is what the new layout looks like, the right child fails
before the replacement is ever consulted:

    [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name
    `_extension` cannot be resolved.

The existing `UnresolvedFallbackIfMissingField` does not help. It catches only
`FIELD_NOT_FOUND`, a missing *struct field*, and not a missing top-level column.
The handle T094 retains therefore needs its own tolerance for an absent
top-level column. T110 as written places tolerance in
`DefaultRepresentation.traverse` only, and `ResourceRepresentation`'s top-level
columns are exactly as prunable.

## Question 3: PASS, with a type caveat

Per-level normalisation works. Each level's step sees the previous level's
output as its left child, which is a resolved `ArrayFilter` over the previous
spike node, or a lambda variable of the `Extension` struct. It then looks up
`_fid` again. Depth 2, depth 3, `extension()`, and both lambda forms all give
the unmodified engine's rows. Nothing expands the recursive type, because
nothing needs to.

**The caveat is about types, and it bears on decision 55's claim that the
strong form holds everywhere.** Per-level normalisation works only because
`_fid` survives in each level's output, so the next step can look it up again.
The normalised `extension` element type is therefore the previous layout's
`Extension` struct, which has:

- `id`, `url` and 50 `value[x]` fields;
- `valueDecimal_scale`, `valueId_versioned`, and the quantity
  `_value_canonicalized` and `_code_canonicalized` fields;
- `_fid`;
- **no inline `extension` field**.

The new layout's `Extension` has no `_fid` and carries `extension` inline
(`contracts/storage-layout.md`). So "every branch yields the same `dataType`"
cannot hold for extension-typed values. More generally, it cannot hold for any
complex element whose `_fid` must survive for a later `.extension` step. The
properties that decision 55 claims to restore therefore do not hold across
layouts for extensions: T089a's assertion that the output type equals the
new-layout type, and the strong form of decision 47's soundness rule. What
does hold is agreement on the FHIRPath-visible type, which is the weak form
decision 47 had originally. This is an observation about the normalised
shape, not a failure of per-level sufficiency. The spike normalised only the
extension step, and did not touch the other discriminators inside the struct.

## Incidental observation

`extension.select(extension.url)` returns empty in both modes, where FHIRPath
gives `urn:ex3_1`. The optimised plan shows why:

    flatten(transform(<level-1 extensions>, x -> filter(_extension[x._fid]).url))

Spark's `flatten` returns null when any inner array is null, and the extensions
with no children produce null inner arrays. This is pre-existing and does not
depend on the spike. It should be raised as a separate defect, not fixed here.

## What this changes downstream

- **Decision 55's premise, and decision 48's second paragraph**, rest on "the
  map is a field on the resource struct". On the current engine it is not. The
  normalisation branch for extensions cannot be unary.
- **`ResolveOrNull` (T038e) needs a binary form for the extension branch**, with
  the resource handle as the second child at every site, not only in lambdas.
  At the resource root it also needs the resource's own `_fid`. T038a should
  cover the binary form over a lambda variable. This spike shows it passes the
  analyzer.
- **T094b's extension branch depends on T094 unconditionally**, and T094a's
  removal of `extensionMapColumn` becomes the move of that handle into T094's
  retained parent/resource handle. It does not go away.
- **T094's handle must tolerate a missing top-level column** on new-layout
  data. T110's tolerance does not reach `ResourceRepresentation`.
- **T089a** cannot assert that the extension output type equals the new-layout
  type while `_fid` must survive for the next step. Either it asserts the weak
  form for extensions, or the branch has to strip `_fid` at a level where
  nothing further can traverse, which the expression cannot know.
- **Alternative for the owner**: represent the resource root as a struct. That
  would make the unary walk viable and restore decision 55 as written. The
  evidence for its cost is thin. In one hand-built control, the optimiser
  collapsed the struct and read `_extension#104` directly. That control put
  the struct in a `Project` below the query, built from explicitly named
  columns. Three things were not tested:
  - the struct inside lambda bodies;
  - the struct under `Filter` or `Aggregate`;
  - the struct as a general representation for the resource root.

  It would also reverse the flat-schema design that `ResourceRepresentation`
  exists to provide, so it is not free.

T038m is left unticked. Under its own rule, the owner decides the replan
against decision 47.

## What this does not answer

- **Plan shapes other than `Project`, for the binary form.** All twelve engine
  scenarios go through the evaluator into `toIdValueDataset`, so each is a
  `Project`. None placed the binary node under `Filter`, `Aggregate` or `Sort`,
  as T009a did for the unary form. The binary form carries one analyzer risk
  that T009a's form did not. Inside a lambda its two children resolve at
  different times: the map attribute in `ResolveReferences`, and the lambda
  variable later, in `ResolveLambdaVariables`. The run found no premature
  forcing, but only in `Project` shapes. `Filter` is a realistic case, because
  `SearchColumnBuilder` emits FHIRPath columns into filters. `Aggregate` is
  where T009a expected canonicalisation to force the replacement. T038a should
  cover the binary form over a lambda variable under `groupBy` and `filter`.

- **Other discriminators.** The spike normalised only the extension step. It
  says nothing about the decimal, quantity or versioned-key branches, which
  involve no second reference and have T009a's unary shape.
- **Fitted schemas.** The spike used the dense previous layout only. On a
  fitted new-layout schema it measured only one thing: that an unguarded map
  handle fails to resolve.
