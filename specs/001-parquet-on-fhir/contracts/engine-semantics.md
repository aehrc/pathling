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

Stored lexically and computed at the documented cap. Comparison, arithmetic,
aggregation and ordering behave as they do today; only the storage changed.

A value whose precision exceeds the computation cap is stored exactly and
computed at the cap. Previously such a value was lost at ingest, so this is a
strict improvement.

## Quantities

Comparison across units uses the `_canonical_exact` annotation when present and
computes the canonicalisation when not. That annotation's precision preserves
magnitude, so quantities differing by orders of magnitude do not compare equal.
The specification's `_canonical` annotation sits beside it and is written for
interchange; the engine does not read it, because its fixed scale loses the
magnitude the comparison depends on.

## Coding and terminology

Coding-valued structures are decoded **by field name**, resolved once per schema
rather than per row. Fields absent from a narrower structure decode as null. A
structure carrying no recognisable field is rejected with an error naming the
expected and the actual fields.

Terminology operations work on a Coding column narrower than the canonical
layout, since a fitted schema commonly carries only the fields the data
populates.

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
