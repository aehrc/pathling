# Data Model: Parquet on FHIR

The stored representation. Written in terms of FHIR types and storage
properties, not Spark classes; the concrete type names appear because they are
the contract.

## Entities

### Resource table

One per FHIR resource type. Its schema is derived by walking the FHIR structural
definitions for that type, then optionally pruned to the elements the data
populates.

| Property | Source | Notes |
| --- | --- | --- |
| Field set | Definitions, pruned by data | Pruning removes branches; it never adds, retypes or re-orders. |
| Field order | Definitions | Identical in pruned and dense modes. |
| Element types | Definitions | Never inferred from data. |
| Cardinality | Definitions | Never inferred from data. |

A resource table never carries `contained` resources, and `Bundle` is never a
resource table.

### Element

A node within a resource. Carries a FHIR type and a cardinality from the
definitions, and is either present in or absent from a given schema.

- **Singular** elements are scalar columns; **repeating** elements are array
  columns. This holds in both schema modes, because cardinality comes from the
  definitions rather than from how many values a batch happened to carry.
- An element is **present** in a pruned schema only if it, or some descendant
  leaf, is populated somewhere in the data. A complex element therefore never
  appears with no fields.
- An element **absent** from the schema is not an error: the definitions
  describe it, the data does not carry it. An element the definitions do not
  describe is a modelling error.
- Absence is detected from the resolved input schema at query time, not baked
  into an expression when it is built, and is represented as a null typed by the
  definitions where the definitions give an unambiguous type — the element's type
  for a singular primitive, an array of it for a repeating one — and by the
  bottom type for a complex element, since only the bottom type widens against an
  arbitrary structure.
- Wherever a structure type is produced, its fields appear in **definition
  order**, restricted to those present. Field order is part of the type, and
  structure equality compares positionally while ignoring names, so a divergent
  order is a silent wrong answer rather than a failure.
- The layout's own fields have positions too, and they must, because they have no
  definition element to take one from. A primitive metadata group and any
  annotations sit **immediately after the element they accompany**, the metadata
  group first and then the annotations in the fixed order the annotation registry
  declares them. Without a stated rule two implementations could both order by
  the definitions and still disagree, which is exactly the positional mismatch the
  previous bullet is there to prevent.

### Primitive metadata group

A primitive element's `id` and `extension`, stored in a group named after the
element with a leading underscore, beside the element itself.

| | |
| --- | --- |
| Named | `_<element>` |
| Contains | `id`, `extension` |
| Present when | The source carried an id or an extension for that element |

This is new capability: the previous layout could not represent it at all,
because its field identifier was per composite and a scalar column had nowhere to
carry one.

### Annotation

A derived value stored beside the element it annotates, accelerating a
computation the engine can also perform from the element itself.

| Annotation | Annotates | Carries |
| --- | --- | --- |
| Numeric | `decimal` | The numeric value of the stored text |
| Range start / end | `date`, `dateTime`, `instant` | The bounds implied by the stated precision |
| Canonical | `Quantity` | The canonicalised value, as the specification types it |
| Canonical exact | `Quantity` | The canonicalised value and unit, at a precision that preserves magnitude |

Properties that hold for every annotation:

- **Optional by definition.** A conformant file may carry none, so the engine
  computes from the annotated element when an annotation is absent.
- **A sibling** of the element it annotates, named after it and positioned
  immediately after it, following the metadata group where one is present, so one
  sibling-resolution mechanism serves annotations and primitive metadata alike.
- **Presence is a schema property**, so whether the fast path or the computation
  is used is decided at planning time rather than per row.
- **Individually disableable by kind**, and disabling one costs performance,
  never correctness. A kind carried in more than one field — the date range's two
  bounds, the quantity's two canonical forms — is governed by one switch.

### Layout

The encoding convention a stored dataset follows, determined from its schema by
structural markers rather than a full comparison. Three outcomes: the new
layout, a previous layout, or unclassifiable. Only the first is readable; the
second is rejected with an actionable message; the third is accepted, because
marker absence is not evidence of a mismatch.

### Schema mode

Whether a schema is pruned to the data or comprises every element the
definitions describe. Both are produced by one traversal of the definitions,
differing only in the strategy that decides which children it descends into and
where it stops — configuration for the dense mode, the observed schema for the
pruned one — so the two cannot diverge in type, cardinality or field order
(decision 68). There is no derived schema: the stored schema is the result of
the columns the transform builds.

The nesting-depth, extension and open-type options bound the dense mode only.
They do not apply to the pruned mode, where depth comes from the data,
extensions appear because they are present, and open types resolve as observed.

The dense mode is delivered in M6, after the annotations and the primitive
metadata group (decision 69). Until then the pruned mode is the only one, the
three options are accepted by the public API and bound nothing, and the schema
mode is not a setting because there is nothing to set it to.

### Strictness switch

*Withdrawn by decision 68.* Content outside the definition set is ignored: the
field is nulled and one warning names what was dropped. There is no setting and
no failing mode, so truncation is reported rather than prevented. The presence
of `contained` resources is reported the same way. Restoring a failing mode, and
validation of values against their declared types, is a named follow-up
(T134g).

## Type mapping

| FHIR type | Stored as | Annotation |
| --- | --- | --- |
| `decimal` | Text, numerically equal to the source but not in its lexical form | Numeric |
| `integer`, `unsignedInt`, `positiveInt` | Integer | — |
| `integer64` | Long | — |
| `boolean` | Boolean | — |
| `date`, `dateTime`, `instant`, `time` | Text, in the lexical form of the source | Range start and end (not for `time`) |
| `string`, `code`, `uri`, `url`, `canonical`, `oid`, `uuid`, `id`, `markdown` | Text | — |
| `base64Binary` | Binary, decoded from the source's base64 | — |
| `Quantity` | The FHIR structure | Canonical, then canonical exact |
| `Coding`, `CodeableConcept` | The FHIR structure | — |
| `Reference` | The FHIR structure | — |
| Complex and backbone types | A structure of their elements | — |

Notes:

- **Decimals are text** because that is what the Parquet on FHIR specification
  stores, and the numeric annotation supplies the value. The *source's* lexical
  form does not survive (decision 68): ingest reads a number with standard JSON
  inference, which yields a double, and stores its text; egress casts that text
  back to a double, so the writer emits a bare number. `1.50` stores as `1.5`
  and forty significant digits store at a double's precision, so trailing zeros,
  exponent notation as written and precision beyond a double are round-trip
  differences, and numbers are compared numerically rather than lexically.
  `evidence/scripts/double_serde.py` records what each form stores and writes.
- **`base64Binary` is binary**, which is what the specification requires: it
  maps the type to Parquet `binary` with no logical type, and Spark's
  `StringType` would write `BINARY` annotated `STRING`. Ingest applies
  `try_to_binary(value, 'base64')`, which decodes whitespace between groups
  and nulls malformed content rather than raising, and egress `base64`, the
  latter with newlines stripped because
  `spark.sql.chunkBase64String.enabled` defaults to true on Spark 4.0.2 and
  chunks at 76 characters. FHIR permits whitespace inside a base64 value, so
  re-encoding canonicalises it away: the same bytes, a different string.
- **A quantity carries two canonical annotations**, the specification's and
  Pathling's, in that order. The specification's types its value as a
  fixed-point decimal whose absolute precision is constant regardless of
  magnitude. Canonicalisation shifts magnitude by arbitrary powers of ten, so a
  mass of one nanogram canonicalises to a value that rounds to zero at that
  precision, and quantities differing by orders of magnitude compare equal —
  silently. That form therefore cannot serve the engine, and the wider one under
  a non-colliding name does. It does not follow that the specification's should
  be dropped: a consumer reading the specification's layout needs the
  specification's annotation under the specification's name, so both are
  emitted. Non-standard annotations are permitted by the specification. Raised
  upstream; the wider annotation is withdrawn if the specification adopts a
  magnitude-preserving representation.
- **No field identifier and no root-level extension map.** Extensions on complex
  elements are inline; extensions on primitives are in the metadata group.
- **No stored versioned reference key.** It is computed from the resource id and
  the version element. If measurement shows a precomputed value is needed, it
  returns as an annotation.

## Round-trip rules

For input conforming to the active definition set, `JSON -> storage -> JSON`
produces a semantically equal resource: object key order ignored, array order
significant, numbers compared **numerically** (decision 68), subject to the
exceptions listed under *What the guarantee covers* below.

Rules the round trip depends on. Each is a defect if violated, except rules 2
and 3, which decision 71 defers:

1. An element absent from the source is **absent** from the output — not present
   and null.
2. A structure whose every field is null in a given row is **omitted**, not
   serialised as an empty object. *Deferred by decision 71, and not checked in
   M1.*
3. An array whose every element is null is **omitted**, not serialised as an
   array of nulls. *Deferred by decision 71, and not checked in M1.*
4. A decimal is emitted as a bare number, not as a quoted string. The stored
   text is cast to a double for the writer, so the emitted form is that
   double's, not the source's.
5. Escapes and Unicode survive unchanged.

Rules 2 and 3 are not hypothetical: the default serialisation behaviour produces
an empty object for an all-null structure and a one-element null array for an
array of nulls, and both differ from the source. M1 writes exactly that, because
it does not prune. For conformant input it happens in two places, both kept on
purpose until M5 stores primitive metadata: a structure whose only content was a
primitive's id and extensions, written as `{}` in its place (decision 72), and a
repeating
primitive's positional null, written without the `_x` array it aligns with
(decision 71's addendum). Non-conformant content produces `{}` as well, and so
can a value in another document that re-types a shared column.

## What the guarantee covers

| Mode | Guarantee |
| --- | --- |
| Pruned | For conformant input, subject to the exceptions below. |
| Dense | The same, and additionally within the configured nesting, extension and open-type bounds. Not available before M6 (decision 69). |

Excluded in both modes. The first three are reported; the rest are not
(decision 68):

- `contained` resources.
- Content the definition set does not describe, or whose JSON encoding type
  contradicts it. Where that was an element's only content, the element is
  written as an empty object if anything else read with it keeps its column,
  and omitted if nothing does, so the document is not conformant FHIR (decision 71).
- Primitive element ids and extensions, until M5. Where they were a structure's
  only content, the structure keeps its place and is written as an empty object,
  because the primitive is stored as a null of its declared type, where the
  metadata group has the outer shape FHIR gives it. A group in another shape,
  including one that another document read with it re-typed, keeps nothing
  (decision 72).
  A repeating primitive's positional nulls are written without the `_x` array
  they align with (decision 71's addendum).
- Decimal lexical form — trailing zeros, exponent notation as written, and
  precision beyond a double. This is no longer a per-path limitation: both
  ingest paths route a number through a double and therefore agree.
- `base64Binary` whitespace, which FHIR permits and which decoding and
  re-encoding canonicalises away.
- A conformant value dropped because a sibling value re-typed its column: one
  `1.5` among integers costs that element for every resource in the file. Where
  it was an element's only content, that element is written as an empty object,
  even in a document that is conformant on its own. Where the re-typed column is
  a primitive's metadata group, the structure it alone kept is written as an
  empty object if anything else read with it keeps its column, and omitted if
  nothing does (decision 72).
- Content the dense bounds drop, which was detectable until `BoundsCheck` was
  removed from M1 (T134f carries the follow-up).
