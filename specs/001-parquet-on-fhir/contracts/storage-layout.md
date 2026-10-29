# Contract: Storage layout

The contract for anyone reading Pathling-written Parquet or Delta files
directly, with their own SQL or another Parquet on FHIR implementation. This
replaces the published schema documentation at
`site/docs/libraries/io/schema.md`.

## Conformance

Files conform to the [Parquet on FHIR](https://github.com/aehrc/parquet-on-fhir)
specification, which requires only that `resourceType` be present and states
that a consuming application SHALL tolerate the absence of any other field.

Deviations, all permitted by the specification:

| Area | What Pathling does |
| --- | --- |
| Decimals | Text plus a numeric annotation, `_x.__numeric`, in the decimal's metadata group rather than the specification's sibling `__x_numeric`, at the specification's `DECIMAL(38,6)`. The text is numerically equal to the source, not in the source's lexical form (decision 68). The text is the stored value, which egress writes. The annotation is the engine's own decoding of the text, at the type the engine computes with (tickets 13 and 41). |
| Primitive ids and extensions | The specification's `_field` groups. |
| Extensions on complex elements | Inline `extension` groups. |
| Date ranges | The specification's start and end annotations, `_x.__start` and `_x.__end`, in the date's metadata group, on `date`, `dateTime` and `instant`. **Deviation (decision 138).** Written by Pathling as `int64` with `TIMESTAMP(isAdjustedToUTC=true, MICROS)`, proleptic Gregorian, rather than the specification's `int96` with `TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS)`, a combination Parquet cannot write; the upper bound is to the microsecond. An `instant` carries them too, which the specification does not name. Lasts until the specification is amended, which ticket 14 drafts. |
| Quantity canonicalisation | **Substitution.** The specification's `__canonical` is not emitted. `__canonical_exact` is emitted in its place, inside the quantity's structure, carrying the same canonicalisation at a precision that preserves magnitude, with the canonicalised unit (decision 137). It is present **from M5** — the layout ships
annotation-free at the flip and each annotation kind lands afterwards (FR-021,
decision 50). The names, types and positions below are fixed from Phase 3 and do
not change when the values arrive. It replaces the specification's because the specification's fixed-point type makes quantities differing by orders of magnitude compare equal, which the engine cannot compare on, and nothing reads the specification's form. Annotations are optional and non-standard ones are permitted, so a consumer of the specification computes what it needs. Raised upstream; `__canonical_exact` is replaced by the specification's annotation if the specification adopts a magnitude-preserving representation. |
| `contained` | Not represented. Dropped silently (FR-006). |
| `Bundle` | Never stored as a resource type. Accepted as a transport carrier and exploded to per-type tables. |

### The decimal annotation

A decimal carries `__numeric` as the last field of its metadata group `_x`,
after `id` and `extension`: `_factorOverride.__numeric`,
`position._latitude.__numeric`, `valueQuantity._value.__numeric`. The group is
stored for the annotation even where the decimal has no id or extension. The
annotation is `decimal(38,6)`, the type the specification gives it and the type
of the engine's System.Decimal (ticket 41), and holds what the engine's decoding
of the text gives: the text rounded to six fractional digits, or null where the
value has more than 32 integer digits or the text is not a decimal (owner,
2026-10-26). The type is the engine's, so that the annotation and the decoding
of the text cannot disagree. The specification places it as a sibling,
`__x_numeric`. The placement is amended by ticket 35.

### The date range annotations

A `date`, `dateTime` or `instant` carries `__start` and `__end` as the last
fields of its metadata group `_x`, after `id` and `extension`, in that order:
`_birthDate.__start`, `meta._lastUpdated.__end`, and for a repeating date one
group per position, `timing._event[].__start`. The group is stored for them even where the date has no id or
extension. They are the earliest and the latest instant the precision the value
states allows, computed from one parse of it by the engine's own date grammar,
so `1980` gives `1980-01-01T00:00:00Z` and `1980-12-31T23:59:59.999999Z`, and a
value stated to the second or beyond gives one instant for both. A time without
a timezone is taken as UTC, as the engine takes it (ticket 14; widening its
range is ticket 26's). Both are Spark's `TimestampType`, so the upper bound is
the latest microsecond.

The specification gives `start` and `end` the physical type `int96` with the
logical type `TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS)`, as siblings, on
`date` and `dateTime`. Parquet allows no logical type on `int96`, so that
combination cannot be written. Spark writes a timestamp as its session's
`spark.sql.parquet.outputTimestampType` says, and offers no writer option for it.
The sessions Pathling builds (Java's default session, Python's managed
defaults and R's `pathling_connect`) set it to `TIMESTAMP_MICROS`, once, as they
are created, so they store the bounds as `int64` with
`TIMESTAMP(isAdjustedToUTC=true, MICROS)`, the true instant on the proleptic
Gregorian calendar, before the Gregorian cutover as after it. A session the user
supplies, or one already active or default where Pathling would otherwise build
one, keeps its own setting, `int96` by default. Where it is not `TIMESTAMP_MICROS`, a
warning is logged when the context is created, and Pathling's Parquet, Delta and
catalog sinks refuse to write data that carries the bounds. At milliseconds the
stored upper bound would disagree with the one the engine computes, and change
the results of comparisons. An `instant` carries them too. These are deviations
until the specification is amended (decision 138); the placement is amended by
ticket 35.

### The quantity annotation

A quantity carries `__canonical_exact` as the last field of its own structure,
once annotations are emitted in M5. Every annotation lives in the container its
element owns: a primitive's in its `_x` group, after `id` and `extension`, and a
complex element's in its structure (decision 135, amending the specification's
sibling placement by ticket 35).

The specification's `__canonical` is not emitted (decision 137).
`__canonical_exact` is Pathling's, and carries the canonicalisation without the
specification's fixed scale, as `struct<value struct<value decimal(38,0), scale
int>, code string>` (ticket 12). It carries the **canonicalised unit code
alongside the value**. Canonicalisation maps to a base
unit, and a value without that unit makes one metre and one second compare
equal, so a single value is not sufficient. The previous Pathling layout used
two fields for exactly this reason.

## The schema is fitted to the data

**This is by design, not a defect.** A resource table carries only the elements
the data populates. Two consequences for a direct consumer:

- A column you expect may be absent, because nothing in the data populated it.
  Absence means "not present in this dataset", never "not part of FHIR".
- The shape of a complex element reflects the data, so the same query over two
  datasets may see structures of different widths.

A dense schema comprising every element the definitions describe is available as
an option, bounded by the configured nesting depth, extension and open-type
settings. It arrives in M6, after the annotations and the primitive metadata
group; the fitted schema is the only one the releases before it write (decision
69).

Element **types and cardinality never vary with the data**: they come from the
FHIR definitions. A repeating element is an array column whether the data
carried one value or a thousand.

## Files within a table may differ

As new elements first appear in incoming data, later files carry columns earlier
files do not. Read the dataset with schema merging enabled, which Pathling's own
sources do by default. A transactional table handles this through its log and
needs nothing extra.

## Caution for direct consumers: unnesting a repeated element

If you write your own query that unnests a repeated element and projects only a
single leaf of it, and some files in the table lack that leaf, **those rows
disappear silently**. Not an error — fewer rows.

The cause is in the Parquet reader rather than in the data: an array's shape is
reconstructed from the repetition levels of the leaves actually read, and a leaf
missing from a file carries none, so the array reads as null and unnesting it
yields nothing.

Three ways to avoid it, in order of preference:

1. Project at least one leaf that every file carries, alongside the one you
   want. Any present sibling restores the array's shape.
2. Unnest the whole element and project its leaves from the unnested rows.
3. Disable nested schema pruning for the query, which is correct but reads every
   leaf of every structure touched.

Pathling's own engine is not affected: it unnests whole elements and never
reduces a read to a single leaf.

## Round trip

For conformant input, `JSON -> storage -> JSON` returns a semantically equal
resource: object key order ignored, array order significant, numbers compared
**numerically**. Decimals are stored as text because the specification stores
them that way, not to preserve the source's lexical form, which JSON ingest
does not preserve (decision 68). During the interim XML ingest keeps it
(decision 134's note of 2026-10-22).

It holds on a fitted schema subject to the exceptions below, and on a dense
schema additionally within the configured nesting, extension and open-type
settings.

Not covered, and none of it reported (FR-016): `contained` resources; an
inline resource held by an element declared as the abstract `Resource`;
primitive element ids and extensions, until M5; decimal lexical form — trailing zeros,
exponent notation as written, and precision beyond a double; `base64Binary`
whitespace, which FHIR permits and which decoding and re-encoding canonicalises
away; and content the dense bounds drop. Content the definition set does not
describe or contradicts is not conformant input, so it is outside the guarantee,
and what ingest stores for it is undefined (FR-063).

Where excluded content was an element's only content, the element is written as
an empty object rather than dropped from its array if anything else read with it
keeps its column, and is omitted if nothing does. Either way the document is not
conformant FHIR. Primitive ids and extensions are the exception to that
condition: a primitive the source carried only as its metadata group, in the
outer shape FHIR gives the group, is stored as a null of its declared type, so
the structure holding it is kept and written as an empty object whatever other
conformant documents are read with it (decision 72). A group in another shape,
or a batch holding one, is not conformant input, and the outcome is undefined
(FR-063). The released encoder also keeps a metadata-only structure where
the primitive is singular, and drops it where the primitive repeats, because the
parser it reads through discards an `_x` array with no `x` array beside it. For
conformant input that is the only source of an empty object, and a repeating
primitive's positional nulls are likewise written without the `_x` array they
align with. Both are kept on purpose until M5 (decision 71 and its addendum).

## Data written by earlier releases

Not readable as this layout, and not upgradable in place. The previous layout
carries a field identifier, a root-level extension map, a decimal scale column, a
versioned identifier column and canonicalised quantity fields, and stores
decimals as a fixed-point type where this layout stores text. (`base64Binary`
is binary in both layouts, so it is not among the differences.) The type change is
not additive: a transactional table refuses the merge, and only a destructive
replace gets past it.

Pathling's sources detect the previous layout and reject it at read time, naming
the resource type, the detected layout and the remedy. Its sinks do the same for
the target they write into, before any schema merge is attempted, so a write
into a dataset written by an earlier release cannot quietly leave one table
carrying both layouts. The check can be disabled per source for the case where
the detector misclassifies conforming data. It does not make previous-layout
data readable: the engine reads only this layout (ADR-0001, FR-046), and makes
no layout check beyond detection at read time (decision 132). Previous-layout
data that bypasses detection (a source with detection disabled, a dataset the
caller supplies, a dataset built with the previous encoder) is unsupported: the
caller owns the consequences, and results are undefined.

Migration of data at rest is by re-import. Where the source still exists,
re-importing it loses nothing. Where it does not, the legacy export (FR-064, the
`legacy-export` module) decodes a previous-layout Parquet or Delta table with the
previous encoder and writes FHIR NDJSON, which the NDJSON source re-imports. An
exported warehouse does not carry the round-trip guarantee: the previous layout
had already rounded decimals to six places and dropped `contained`, among the
losses the module documents. A re-imported one does. The site's storage layout
page describes the refusal, the unsupported bypass routes and both migration routes (decision 117,
ticket 07).
