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
| Decimals | Lexical text plus the specification's numeric annotation. |
| Primitive ids and extensions | The specification's `_field` groups. |
| Extensions on complex elements | Inline `extension` groups. |
| Date ranges | The specification's start and end annotations. |
| Quantity canonicalisation | **Deviation.** Pathling emits its own canonical annotation under a non-colliding name, at a precision that preserves magnitude. The specification's own canonical annotation is not emitted, because its fixed-point type makes quantities differing by orders of magnitude compare equal. Raised upstream; withdrawn if the specification adopts a magnitude-preserving representation. The specification's form can be added additively later if an interchange consumer needs it. |
| `contained` | Not represented. Detected, never silently dropped. |
| `Bundle` | Never stored as a resource type. Accepted as a transport carrier and exploded to per-type tables. |

## The schema is fitted to the data

**This is by design, not a defect.** A resource table carries only the elements
the data populates. Two consequences for a direct consumer:

- A column you expect may be absent, because nothing in the data populated it.
  Absence means "not present in this dataset", never "not part of FHIR".
- The shape of a complex element reflects the data, so the same query over two
  datasets may see structures of different widths.

A dense schema comprising every element the definitions describe is available as
an option, bounded by the configured nesting depth, extension and open-type
settings.

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
lexically — which is why decimals are stored as text.

Unconditional on a fitted schema. On a dense schema, bounded by the configured
nesting, extension and open-type settings, with content those bounds would drop
detectable rather than silently lost.

Not covered, and detected rather than silent: `contained` resources; content the
definition set does not describe; and the lexical form of decimals supplied
through the API that takes resources as a dataset of strings.

## Data written by earlier releases

Not readable as this layout, and not upgradable in place. The previous layout
carries a field identifier, a root-level extension map, a decimal scale column, a
versioned identifier column and canonicalised quantity fields, and stores
decimals as a fixed-point type where this layout stores text. The type change is
not additive: a transactional table refuses the merge, and only a destructive
replace gets past it.

Pathling's sources detect the previous layout and reject it at read time, naming
the resource type, the detected layout and the remedy. The check can be disabled
per source for the case where conforming data cannot be classified.

Migration of data at rest is separate work: a version gate by default, and an
opt-in rewrite tool built from the retained previous implementation. A migrated
warehouse does not carry the round-trip guarantee — the previous layout had
already truncated long decimals and dropped `contained`. A re-imported one does.
