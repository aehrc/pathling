# Contract: Library API

What changes, and does not change, in the public API of the library in Java,
Python and R.

## Preserved unchanged

| Surface | Note |
| --- | --- |
| Encoding resources from strings | Signature and semantics preserved in all three languages. |
| Encoding bundles | Preserved. Bundles remain transport carriers, exploded to per-type tables. |
| Decoding resources to strings | Preserved. |
| XML input | Preserved, by converting to JSON before ingest. |
| Reading and writing Parquet, Delta, catalog tables and NDJSON | Preserved. |
| Terminology operations on Coding columns | Signatures and behaviour unchanged for correctly shaped input. |
| Evaluating FHIRPath to a column, and against a single resource | Signatures preserved. |
| Running ViewDefinitions | Preserved. |

No public signature is removed or narrowed.

## Options added

| Option | Meaning | Default |
| --- | --- | --- |
| Schema mode | Whether the stored schema is fitted to the data or comprises every defined element. | Fitted |
| Strictness | Whether content outside the definition set is ignored or raises an error. | *Chosen at implementation; must be stated in the documentation.* |
| Annotation toggles | Enable or disable each annotation individually. | All enabled |
| Layout detection opt-out | Per source, for conforming data the detector cannot classify. | Detection enabled |

All are surfaced in Java, Python and R.

## Options whose meaning is unchanged but whose reach narrows

The nesting-depth, extension and open-type options keep their current semantics
for the dense schema. They do not apply to the fitted schema, where depth comes
from the data, extensions appear because they are present, and open types
resolve as observed.

This is neither a deprecation nor a reinterpretation. Set them and switch to the
fitted mode, and they simply have nothing to bound.

## Behaviour changes

### Reading data written by an earlier release now fails

Previously an old file flowed into the engine and produced a confusing failure,
or silently wrong answers where column shapes happened to overlap. It is now
rejected at read time with a message naming the resource type, the detected
layout, the expected layout and the remedy.

This is the intended improvement, but it is a behaviour change and belongs in
the release notes. The per-source opt-out exists for conforming data the
detector cannot classify.

### A column expression stays valid over any conformant schema

The expression-to-column API keeps its current contract: an expression becomes a
column without reference to any dataset, and that column can be applied to any
dataset conforming to the definitions. This survives the move to schemas fitted
to the data. One column applied to a fitted dataset and to a dense dataset
holding the same resources gives equal results.

Where the element is absent from a dataset's schema, the column is a null of the
type the definitions give that element — not an untyped column. So it can be
written to Parquet, and its reported type is the element's FHIR type rather than
the null type. Absent *complex* elements are represented internally by the bottom
type. That can reach a caller through the expression-to-column API — an
expression whose result is a complex element the dataset does not carry — and a
bottom-typed column **cannot be written to Parquet or ORC**, and is **omitted**
rather than null when written to JSON. It cannot reach a ViewDefinition column,
because view columns are primitive. This is the one residual of the untyped-column
problem, narrowed from every absent element to absent complex ones.

This is a change from an earlier draft of this design, which returned an untyped
column and required a ViewDefinition to declare a type to recover one.

### A view column over an absent element no longer needs a declared type

A ViewDefinition column that declares a FHIR type produces an output column of
that type whether or not the element is present in the data — unchanged.

A column that declares *no* type, over an absent element, now also succeeds: the
definitions supply the type. Declaring column types remains good practice and is
what shareable views do, but it is no longer the difference between a view that
runs and one that fails on fitted data.

### Decimals

Stored exactly as supplied. Computed at the same precision as today, which is
documented as a cap permitted by the FHIR specification's deference to XML
Schema.

**Limitation.** Resources supplied as a dataset of strings do not preserve the
lexical form of decimals: trailing zeros are dropped, exponent notation is
normalised, and a value beyond the precision of a double loses significant
digits. This is a defect in the underlying JSON reader, not in Pathling — an
incomplete fix for an upstream issue, which has been reported. Supplying
resources as files preserves the lexical form exactly.

### Schema merging on by default

Reading a dataset of raw files merges their schemas by default, because
otherwise the first file's schema wins and the rest are silently null-filled.
Merging reads every file's metadata, which costs time on large datasets; it can
be disabled where the files are known to be uniform.

Appending to a transactional table merges the schema, so a batch carrying new
elements widens the table rather than failing.

Upserting into a transactional table also widens it. This is a **behaviour
change**: the upsert path previously refused a source carrying elements the target
lacked, failing on the structure mismatch, so that tolerance could not become
schema evolution the caller had not asked for. That refusal assumed a schema
derived from encoding configuration and therefore stable between batches. Under a
schema fitted to the data it would fire on ordinary use, and append and upsert
would answer the same question two ways.
