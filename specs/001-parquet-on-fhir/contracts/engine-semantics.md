# Contract: Engine semantics

What FHIRPath evaluation guarantees over the new layout. Everything here is
additional to the FHIRPath specification and its FHIR bindings, which are
unchanged.

## Absent elements

An element the FHIR definitions describe but the stored schema does not carry
evaluates to an **empty collection**. It is not an error: the definitions
describe it, this dataset does not populate it.

An element the definitions do not describe remains an **error**. That distinction
is the point of deriving schemas from definitions: "in the definitions but not in
this schema" means absent from the data, and "not in the definitions" is a
modelling error.

Selecting a choice variant absent from the schema yields empty. Where only one
variant was ever populated, selecting another by type returns empty rather than
failing.

Combining an absent element with a populated one succeeds. Union, combination,
conditional selection and comparison all work across the two.

### When absence is decided, and how it is represented

Presence is decided from the **resolved input schema**, not when the expression is
built. An expression is still converted to a column with no reference to any
dataset, and the resulting column is valid over every schema that conforms to the
definitions — fitted or dense, written by Pathling or by any other conformant
writer. Applied to two datasets holding the same resources under different
schemas, one column gives equal results.

An absent element is a null typed by one principle: **the definitions' type where
that type is unambiguous, the bottom type where a concrete shape would
over-constrain later combination.**

| Absent element | Type |
| --- | --- |
| Singular primitive | the type the definitions give it |
| Repeating primitive | an array of that type |
| Singular complex | the bottom type |
| Repeating complex | an array of the bottom type |

A concrete minimal structure is *not* used for complex elements. It looks
harmless and is not: only the bottom type widens against an arbitrary structure,
so a minimal structure fails the moment an absent complex element has to be
combined with a populated one.

## Cardinality

Singular elements are scalar columns, repeating elements are array columns, and
this follows the definitions rather than the data. A repeating element that
happens to carry one value in every row is still an array, in both schema modes.

Empty propagates through most operations. Singleton coercion errors on
multi-item input rather than taking the first.

## Annotations

Annotations are optional in the specification, so a conformant file may carry
none. **The engine computes from the annotated element when an annotation is
absent**, and uses the annotation only as a fast path when present.

This is not a hypothetical case. The layout ships annotation-free at the flip and
each kind lands afterwards in M5, so for that period the computed path is the
only path, and every statement below about an annotation being present describes
M5 onward.

An engine that required annotations could read only files Pathling itself wrote,
which would defeat adopting the format. The full test suite therefore passes over
files written with every annotation disabled.

Which path is taken is decided from the schema, not per row, so the choice is
made at planning time.

## Result types

| Result | Type behaviour |
| --- | --- |
| Primitive, element present | The Spark type for its FHIR type. |
| Primitive, element absent from the schema | The same type, with a null value. |
| Complex, element present | Reflects the stored schema, which is fitted to the data. |
| `System.Quantity`, built by the engine | One fixed shape, whatever produced it (see [Quantities](#quantities)). |
| `System.Coding`, built by the engine | One fixed shape, whatever produced it (see [Coding and terminology](#coding-and-terminology)). |
| Complex, element absent | The bottom type, which never reaches a view column. |
| View column declaring a FHIR type | That type, present or absent. |
| View column declaring no type, primitive element absent | The definitions' type for the element. |
| Search filter | Boolean, always. |
| Single-resource evaluation | Materialised values with FHIRPath type names; no Spark type is exposed. |

So no untyped column reaches a caller for an absent primitive, and the type a
caller sees does not depend on whether the dataset happens to populate the
element. A view column that declares no type over an absent element therefore
succeeds, where an earlier draft of this design had it fail.

Deriving an output type still fails, with a message naming the column, its path
and the remedy, for a column that genuinely carries no type information. An
element absent from the schema is no longer such a column.

## Decimals

Stored as text and computed at the documented cap, the engine's System.Decimal,
`DECIMAL(38,6)`: six fractional digits and 32 integer digits (FR-035). It was
`DECIMAL(32,6)` before ticket 41. Comparison, arithmetic, aggregation and
ordering behave as they did, except that a value with 27 to 32 integer digits,
null at the earlier cap, now has a value, and a decimal leaves the engine as
`DECIMAL(38,6)`. FHIRPath asks for eight fractional digits; the six kept here
are a known deviation (FR-035).

The stored text is not the source's lexical form (decision 68). Ingest reads a
number with standard JSON inference, which yields a double, and stores its
text; a value beyond a double's precision is therefore stored at a double's
precision rather than exactly. Computation is unaffected — it reads the text and
decodes at the cap, as before — so what changed is how much precision reaches
storage, not how much survives a calculation.

## Quantities

A quantity read from the data keeps the fitted shape of the data it came from,
with its extensions. A `System.Quantity` — one the engine builds itself, such as
a literal, a number promoted to a quantity, the result of `toQuantity()`, or a
stored quantity combined with one of these — has one fixed, complete shape,
whatever produced it (FR-059):

| Field | Type | Holds |
| --- | --- | --- |
| `id` | string | Null, except for a stored quantity taken as a System one, which keeps its id. |
| `value` | string | The text of the decimal, as the layout stores a decimal, with the digits it was given. A number promoted to a quantity has the six fractional digits of the engine's decimal type. |
| `_value` | struct of `__numeric` decimal(38,6) | The value's metadata group, as the layout stores one, carrying only the numeric annotation: the value as the engine's System.Decimal, null where the text does not decode to it. |
| `comparator` | string | Null, except for a stored quantity taken as a System one. |
| `unit` | string | The human-readable unit. |
| `system` | string | The system of the unit's code. |
| `code` | string | The unit's code. |
| `__canonical_exact` | struct of `value` (struct of the unscaled `value` decimal(38,0) and its `scale` int) and `code` string | The exact canonical annotation, null where the quantity has no canonical form. |

These are the elements of a FHIR Quantity other than `extension`, in definition
order, and every one is present, with the value's metadata group and its numeric
annotation right after `value`, as the layout stores it (ticket 41, amending
FR-059), followed by the layout's exact canonical annotation (ticket 12, amending
FR-059). A System quantity has no extensions. It carries no decimal value with a
scale companion and no field identifier, so an engine-built quantity never looks
like a previous-layout one.

The numeric value and the canonical form are carried so that each is computed
once for a quantity and travels with it through unions, arrays and lambdas. A
literal has them as constants, filled when the plan is built. A stored quantity
taken as a System one copies its stored `_value.__numeric` and
`__canonical_exact` where the schema carries them and computes them otherwise.
A conversion or a number promoted to a quantity computes them. The numeric value
is computed as the engine decodes a decimal's text, so reading it gives what
decoding the text gives. The value of a quantity, stored or System, is read from
`_value.__numeric` wherever the engine computes or compares with it, and a
search matches a stored quantity's value the same way.
Comparison and the set operations read the carried form rather than
canonicalising again. Where it is computed, it is computed by the quantity kind
that ingest stores it with (decision 135).

The exact canonical annotation leaves the engine with the System quantity (owner,
2026-10-26): a column returned to a caller keeps it, as a stored quantity returned in its fitted
shape keeps the annotations it is stored with. JSON and XML egress leave out
every `__` field, and so does the rendering of a single-resource evaluation. The
annotation switches govern only what ingest stores (decision 135, part 1), so
they do not change this shape. The shape of SQL output is specified by ticket 40.

Comparison across units uses the `__canonical_exact` annotation when present and
computes the canonicalisation when not. That annotation's precision preserves
magnitude, so quantities differing by orders of magnitude do not compare equal.
The specification's `__canonical` annotation, whose fixed scale loses that
magnitude, is neither stored nor carried (decision 137).

## Coding and terminology

Coding-valued structures are decoded **by field name**, resolved once per schema
rather than per row. Fields absent from a narrower structure decode as null. A
structure carrying no recognisable field is rejected with an error naming the
expected and the actual fields.

Terminology operations work on a Coding column narrower than the canonical
layout, since a fitted schema commonly carries only the fields the data
populates.

A Coding read from the data keeps the fitted shape of the data it came from,
with its extensions. A `System.Coding` — one the engine builds itself, such as a
Coding literal, the result of a terminology function such as `translate()` or
`property()`, or the Coding a quantity's coded unit is taken as — has one fixed,
complete shape, whatever produced it (FR-059):

| Field | Type | Holds |
| --- | --- | --- |
| `id` | string | The element id, null unless the source Coding had one. |
| `system` | string | The code system. |
| `version` | string | The version of the code system. |
| `code` | string | The code. |
| `display` | string | The display text. |
| `userSelected` | boolean | Whether the Coding was chosen directly by the user. |

These are the elements of a FHIR Coding other than `extension`, in definition
order, and every one is present. A System Coding has no extensions and no field
identifier: it no longer carries the previous layout's `_fid`, so an
engine-built Coding never looks like a previous-layout one. Telling
`System.Coding` apart from `FHIR.Coding` in the type system is
[#2804](https://github.com/aehrc/pathling/issues/2804); until then, a stored
Coding combined with an engine-built one is reconciled with it by name and keeps
its extensions (decision 79's addendum).

## References

`resolve()` returns the referenced resources, joined by resource key computed
from the conformant identifier elements. It does not depend on a stored
versioned-key column. If a precomputed key proves necessary for join
performance, it returns as an annotation, and the engine still works without it.

An unresolvable reference, a reference to a resource type absent from the data,
and an absent reference all yield empty rather than raising.

## Primitive ids and extensions

Navigable, for the first time. A primitive element's id and extensions resolve
from the metadata group stored beside it, and yield empty where the source
carried neither.

**Delivered in M5, not at the flip.** Neither the writing nor the reading lands
with the layout switch: the previous layout cannot represent this content and the
engine cannot navigate it today, so deferring both regresses nothing. Until then
the metadata group is not written, and the content it would carry is an
exception to the round-trip guarantee (FR-016), dropped silently.

## Combining collections whose shapes differ

Two collections of the same FHIR type reached by different paths — `Patient.name`
and `Patient.contact.name`, say — have the same FHIR type but, under a fitted
schema, different SQL shapes, because the data populates different elements in
each place. Every operation that needs both sides to share a type reconciles them
first: `combine`, `|`, equality and comparison of complex values, conditional
selection, and membership.

Reconciliation projects each side **by name** into the merged type, which is the
recursive field-wise union of the input types. Fields a side lacks become null.
The result holds every element of both sides and can be traversed exactly as
either side could.

Reconciliation is never a cast. A struct cast of differing arity is refused, but a
cast between structures of equal arity **reorders fields positionally and ignores
their names**, which would compare or combine the wrong fields with no error at
all. This is the same class of defect as positional Coding decoding.

The FHIR-type promotion that already exists — integer to decimal, for instance —
is unchanged and remains a separate, definition-driven step. Shape reconciliation
happens after it.

## Canonical field order

Wherever a structure type is produced — schema derivation, the merge of two
element types, the merge of divergent file schemas, reconciliation — fields
appear in **definition order**, restricted to those present.

This is load-bearing, not cosmetic. Field order is part of a structure's type, so
two orderings of the same fields have no common type and cannot be combined. Worse,
they *can* be compared: structure equality coerces positionally and ignores field
names, so two values whose schemas order fields differently compare equal when
they are not. Canonical order makes that unreachable within Pathling. Data written
elsewhere may still be ordered differently, which is why reconciliation projects
by name rather than trusting the order it is given.

## Constraint: how unnesting is implemented

Unnesting a repeating element **must** operate on whole elements, and must never
be reshaped so that the read is reduced to a single leaf of the element.

This is not a style preference. Where files within a table differ — the steady
state once schemas are fitted to data — a read reduced to a leaf that some file
lacks returns *no rows at all* for that file's resources, silently, because the
array's shape is reconstructed from the repetition levels of the leaves actually
read. Whole-element access keeps those levels available.

The engine satisfies this today. It is pinned by a regression test over a
fixture whose files deliberately disagree, so a future optimisation that pushes
leaf selection into the read fails the build rather than silently losing
clinical data.
