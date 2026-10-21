# How much wider annotations make a dense schema

Measured for ticket 11, which decides whether a dense schema carries an
annotation for every element the definitions describe or only for the elements
the data populates (decision 135).

**Result: on a dense `Observation` without extensions, annotating every
described element adds about 305 fields to about 1,275 element leaves, roughly
24%.** The ratio barely moves with the nesting bound, because annotated types
are mostly leaves of composite types that recur at every depth.

## Method

`scripts/DenseAnnotationWidth.java` walks `DefinitionCanonicalStructure` for a
resource from the FHIR R4 definitions and counts the entries it reaches:

- every primitive element, and the resource type field, as an element leaf;
- each annotation entry, by kind, as one field;
- each metadata group, reported separately and not counted as a leaf.

A composite type is descended into while it appears on the path no more than
`maxNesting` times, which approximates the nesting bound of `EncodingConfiguration`.
Where extensions are enabled, an `Extension`'s `value[x]` is restricted to the
standard open types. Run it with the `fhir-schema` classes and their dependencies
on the classpath:

```
java DenseAnnotationWidth Observation <maxNesting> <extensions>
```

Measured at `cbce542dd5`, Spark 4.0.2, Java 21.

## Results

`Observation`, extensions off:

| Nesting bound | Element leaves | Numeric | Range (start + end) | Canonical | Canonical exact | Annotation fields | Increase |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 548 | 31 | 80 | 19 | 19 | 149 | 27% |
| 1 | 819 | 35 | 128 | 23 | 23 | 209 | 26% |
| 2 | 1,047 | 35 | 176 | 23 | 23 | 257 | 25% |
| 3 | 1,275 | 35 | 224 | 23 | 23 | 305 | 24% |

Each annotation is counted as one field. The canonical exact annotation carries
a value and a unit, so if ticket 12 stores it as a structure it adds 23 more
leaves at every bound. The metadata groups, which ticket 17 adds, number one per
primitive element (1,274 at bound 3) and are a separate question.

## Caveats

- **This does not reproduce the 1,371 leaves of `merge-cost.md`.** That figure
  came from `SchemaBuilder.dense` at the default bounds, which enable extensions,
  and `SchemaBuilder` has since been removed. This walk with extensions off gives
  1,275 at bound 3. The two are close, but they were not produced by the same
  bound, so the percentages are the figure to rely on, not the absolute counts.
- **With extensions on, this walk does not terminate in a useful size.** At bound
  3 it reaches about 206 million element leaves, and at bound 0 still 7,151,
  because an extension is itself extensible and its `value[x]` opens composite
  types that carry extensions again. The previous encoder bounded extensions in a
  way this walk does not copy. The annotations grow in the same proportion as the
  rest of the schema, so the proportion above still holds, but the dense mode
  needs an extension bound of its own: ticket 18.
