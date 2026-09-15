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
| Numeric | `decimal` | The numeric value of the lexical form |
| Range start / end | `date`, `dateTime`, `instant` | The bounds implied by the stated precision |
| Canonical | `Quantity` | The canonicalised value and unit, at a precision that preserves magnitude |

Properties that hold for every annotation:

- **Optional by definition.** A conformant file may carry none, so the engine
  computes from the annotated element when an annotation is absent.
- **A sibling** of the element it annotates, named after it and positioned
  immediately after it, following the metadata group where one is present, so one
  sibling-resolution mechanism serves annotations and primitive metadata alike.
- **Presence is a schema property**, so whether the fast path or the computation
  is used is decided at planning time rather than per row.
- **Individually disableable**, and disabling one costs performance, never
  correctness.

### Layout

The encoding convention a stored dataset follows, determined from its schema by
structural markers rather than a full comparison. Three outcomes: the new
layout, a previous layout, or unclassifiable. Only the first is readable; the
second is rejected with an actionable message; the third is accepted, because
marker absence is not evidence of a mismatch.

### Schema mode

Whether a schema is pruned to the data or comprises every element the
definitions describe. The dense mode is the same derivation with pruning
skipped, so the two cannot diverge in type, cardinality or field order.

The nesting-depth, extension and open-type options bound the dense mode only.
They do not apply to the pruned mode, where depth comes from the data,
extensions appear because they are present, and open types resolve as observed.

### Strictness switch

How content outside the definition set is treated: ignored, or an error naming
it. Silent truncation is not an outcome. The presence of `contained` resources is
governed by the same switch, so that carve-out is never silent.

## Type mapping

| FHIR type | Stored as | Annotation |
| --- | --- | --- |
| `decimal` | Text, in the lexical form of the source | Numeric |
| `integer`, `unsignedInt`, `positiveInt` | Integer | — |
| `integer64` | Long | — |
| `boolean` | Boolean | — |
| `date`, `dateTime`, `instant`, `time` | Text, in the lexical form of the source | Range start and end (not for `time`) |
| `string`, `code`, `uri`, `url`, `canonical`, `oid`, `uuid`, `id`, `markdown` | Text | — |
| `base64Binary` | Text | — |
| `Quantity` | The FHIR structure | Canonical |
| `Coding`, `CodeableConcept` | The FHIR structure | — |
| `Reference` | The FHIR structure | — |
| Complex and backbone types | A structure of their elements | — |

Notes:

- **Decimals are text** so that the lexical form survives: trailing zeros,
  exponent notation, a leading sign and arbitrary digit counts all compare
  lexically on the round trip. The numeric annotation supplies the value.
- **The canonical quantity annotation deviates from the specification's**, which
  types its value as a fixed-point decimal whose absolute precision is constant
  regardless of magnitude. Canonicalisation shifts magnitude by arbitrary powers
  of ten, so a mass of one nanogram canonicalises to a value that rounds to zero
  at that precision, and quantities differing by orders of magnitude compare
  equal — silently. Pathling emits its own wider annotation under a
  non-colliding name. Non-standard annotations are permitted by the
  specification. Raised upstream; withdrawn if the specification adopts a
  magnitude-preserving representation.
- **No field identifier and no root-level extension map.** Extensions on complex
  elements are inline; extensions on primitives are in the metadata group.
- **No stored versioned reference key.** It is computed from the resource id and
  the version element. If measurement shows a precomputed value is needed, it
  returns as an annotation.

## Round-trip rules

For input conforming to the active definition set, `JSON -> storage -> JSON`
produces a semantically equal resource: object key order ignored, array order
significant, numbers compared lexically.

Rules the round trip depends on, each of which is a defect if violated:

1. An element absent from the source is **absent** from the output — not present
   and null.
2. A structure whose every field is null in a given row is **omitted**, not
   serialised as an empty object.
3. An array whose every element is null is **omitted**, not serialised as an
   array of nulls.
4. A decimal is emitted in the lexical form that was stored.
5. Escapes and Unicode survive unchanged.

Rules 2 and 3 are not hypothetical: the default serialisation behaviour produces
an empty object for an all-null structure and a one-element null array for an
array of nulls, and both differ from the source.

## What the guarantee covers

| Mode | Guarantee |
| --- | --- |
| Pruned | Unconditional, for conformant input. |
| Dense | Within the configured nesting, extension and open-type bounds; content those bounds would drop is detectable rather than silently lost. |

Excluded in both modes, and detected rather than silent:

- `contained` resources.
- Content the definition set does not describe.
- Lexical decimal form on the ingest path that takes resources as a dataset of
  strings — a defect in the underlying JSON reader, reported upstream, documented
  per path.
