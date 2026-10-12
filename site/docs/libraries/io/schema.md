---
sidebar_position: 3
description: How Pathling stores FHIR resources in Parquet and Delta, for anyone reading the files directly.
---

# Storage layout

This page describes how Pathling stores FHIR resources when it writes them to
[Parquet](https://parquet.apache.org/) files, [Delta Lake](https://delta.io/)
tables or managed tables. It is written for anyone who reads those files
directly, with their own SQL or with another tool, rather than through the
Pathling [data sources](./index.md#reading-fhir-data).

## Conformance

Files written by Pathling conform to the
[Parquet on FHIR](https://github.com/aehrc/parquet-on-fhir) specification. That
specification requires only that `resourceType` be present, and states that a
consuming application must tolerate the absence of any other field.

Pathling stores [FHIR R4](https://hl7.org/fhir/R4) resources. Every R4 resource
type can be stored except `Bundle`, which is treated as a carrier of other
resources: reading bundles extracts the resources they hold into a table for
each resource type.

## Schema structure

Each resource type is stored as a table of its own, with one row per resource.
The table's schema follows the [FHIR JSON](https://hl7.org/fhir/R4/json.html)
representation of the resource:

- **Element names** are the names used in FHIR JSON, starting with
  `resourceType` and `id` at the root.
- **Complex and backbone elements** are structures (Parquet groups) of their
  child elements.
- **Repeating elements** are arrays, and singular elements are scalar values.
  Cardinality comes from the FHIR definitions, never from the data: a repeating
  element is an array column whether the data carried one value or a thousand.
- **Choice elements** such as `value[x]` are stored as one field per type, named
  as in FHIR JSON (`valueString`, `valueQuantity`, and so on), in the place the
  choice element occupies.
- **Extensions on complex elements and resources** are stored inline, as an
  `extension` array of `Extension` structures within the element they extend,
  exactly as in FHIR JSON.

### Primitive types

| FHIR type                                                                    | Stored as           | Notes                                                                            |
| ---------------------------------------------------------------------------- | ------------------- | -------------------------------------------------------------------------------- |
| `boolean`                                                                    | `BOOLEAN`           |                                                                                  |
| `integer`, `unsignedInt`, `positiveInt`                                      | `INT32`             |                                                                                  |
| `decimal`                                                                    | `BINARY` (`STRING`) | Text that is numerically equal to the source value. See [Decimals](#decimals).   |
| `date`, `dateTime`, `instant`, `time`                                        | `BINARY` (`STRING`) | Text, in the form the source gave it. An `instant` is text too, not a timestamp. |
| `string`, `code`, `id`, `uri`, `url`, `canonical`, `oid`, `uuid`, `markdown` | `BINARY` (`STRING`) |                                                                                  |
| `xhtml`                                                                      | `BINARY` (`STRING`) |                                                                                  |
| `base64Binary`                                                               | `BINARY`            | The bytes the base64 text encodes, with no logical type.                         |

### Decimals

A decimal is stored as text, as the specification requires, but the text is
not necessarily the text the source used. Pathling reads a decimal as a number
and stores the text of that number, so the stored value is numerically equal to
the source but not lexically identical to it:

- trailing zeros are dropped (`1.50` is stored as `1.5`);
- exponent notation is normalised, and very small or very large values may be
  stored in exponent form (`0.0000001` is stored as `1.0E-7`);
- a value with more significant digits than a double-precision number holds
  loses the extra digits;
- the same value may be stored as `100` or as `100.0`, depending on the other
  decimal values read alongside it.

If you query decimals directly, parse them as numbers rather than comparing
their text. Pathling's own queries decode the text to a decimal with a
precision of 32 and a scale of 6.

### What is not stored

The following content is not stored. When it is encountered, it is dropped, and
a warning naming what was dropped is logged. There is no option that makes it
fail instead.

- `contained` resources.
- Elements that the FHIR definitions do not describe.
- Values whose JSON type contradicts the type the definitions give the element,
  such as a string where the definitions call for a boolean. See
  [Encoding behaviour](./encoders.md#encoding-behaviour) for how far this
  reaches.
- The `id` and `extension` of primitive elements, which FHIR JSON carries in a
  `_`-prefixed property beside the element (for example `_birthDate`). The
  element's value is kept.

This release does not write the optional annotation fields that the
specification defines (fields prefixed with `__`, such as the numeric value of
a decimal or the canonical form of a quantity), nor the `_`-prefixed groups that
the specification uses for the ids and extensions of primitive elements.

## The schema is fitted to the data

This is by design, not a defect. A table carries only the elements that the
data populates. This has two consequences for anyone reading the files
directly:

- A column you expect may be absent, because nothing in the data populated it.
  Absence means "not present in this dataset", never "not part of FHIR".
- The shape of a complex element reflects the data, so the same query over two
  datasets may see structures of different widths.

The types and cardinality of the elements that are present never vary with the
data, because they come from the FHIR definitions.

### Field order

Within every structure that Pathling writes, the fields appear in the order that
the FHIR definitions declare them, leaving out those that are absent. The fields
of a choice element follow the order of its types in the definitions. For an
open choice, such as the value of an extension, that is the order in which the
FHIR specification lists the types an extension value can take.

That order holds for each file as it is written, but not necessarily for a
schema assembled from several of them. When Spark merges the schemas of
differing Parquet files, and when a Delta table is widened to hold new columns,
a field that first appears later is placed at the end of its structure.

Field order is part of a Parquet or Spark structure type, and some operations
compare structures by position rather than by name. Do not assume that two
structures of the same FHIR type have the same fields in the same positions:
compare and combine them by field name.

## Files within a table may differ

As new elements first appear in incoming data, later files carry columns that
earlier files do not. Read a directory of Parquet files with schema merging
enabled (in Spark, the `mergeSchema` option), so that every file's columns are
read. Pathling's own Parquet source does this by default. A Delta table records
its schema in its transaction log and needs nothing extra.

## Caution: unnesting a repeated element

If you write your own query that unnests a repeated element and projects only a
single field of it, and some files in the table lack that field, **the rows from
those files disappear silently**. There is no error, just fewer rows.

The cause lies in the Parquet reader rather than in the data. The reader
reconstructs the shape of an array from the fields it actually reads. A field
that is missing from a file carries nothing to reconstruct it from, so the array
reads as null in that file, and unnesting it yields no rows.

There are three ways to avoid this, in order of preference:

1. Project at least one field that every file carries, alongside the one you
   want. Any field that is present restores the shape of the array.
2. Unnest the whole element, and select its fields from the unnested rows.
3. Disable nested schema pruning for the query (in Spark,
   `spark.sql.optimizer.nestedSchemaPruning.enabled`). This is correct, but it
   reads every field of every structure the query touches.

Queries run through Pathling, including FHIRPath expressions and SQL on FHIR
views, are not affected.

## Round trip

For conformant input, encoding FHIR JSON with Pathling and writing it back out
as FHIR JSON returns an equal resource, where object key order is ignored, array
order is significant and numbers are compared by value. The exceptions are:

- the content listed under [What is not stored](#what-is-not-stored);
- the lexical form of decimals, as described under [Decimals](#decimals);
- whitespace within `base64Binary` values, which FHIR permits and which is lost
  when the value is decoded and encoded again;
- a value dropped because another value of the same element, read alongside it,
  contradicted the element's type.

Where dropped content was the only content of a structure, the structure may be
written as an empty object, so the output is not always conformant FHIR.

## Example

The following is the schema of a `Patient` table written by Pathling from a
small sample of synthetic patients, as Spark reports it. Only the elements that
the sample populates are present, and `multipleBirthBoolean` and
`deceasedDateTime` are the fields of choice elements that the sample uses.

```
root
 |-- resourceType: string (nullable = true)
 |-- id: string (nullable = true)
 |-- text: struct (nullable = true)
 |    |-- status: string (nullable = true)
 |    |-- div: string (nullable = true)
 |-- extension: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- extension: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- url: string (nullable = true)
 |    |    |    |    |-- valueString: string (nullable = true)
 |    |    |    |    |-- valueCoding: struct (nullable = true)
 |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |-- url: string (nullable = true)
 |    |    |-- valueCode: string (nullable = true)
 |    |    |-- valueDecimal: string (nullable = true)
 |    |    |-- valueString: string (nullable = true)
 |    |    |-- valueAddress: struct (nullable = true)
 |    |    |    |-- city: string (nullable = true)
 |    |    |    |-- state: string (nullable = true)
 |    |    |    |-- country: string (nullable = true)
 |-- identifier: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- type: struct (nullable = true)
 |    |    |    |-- coding: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |-- system: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- name: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- use: string (nullable = true)
 |    |    |-- family: string (nullable = true)
 |    |    |-- given: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- prefix: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- suffix: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |-- telecom: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- system: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |    |    |-- use: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- birthDate: string (nullable = true)
 |-- deceasedDateTime: string (nullable = true)
 |-- address: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- extension: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- extension: array (nullable = true)
 |    |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |    |-- url: string (nullable = true)
 |    |    |    |    |    |    |-- valueDecimal: string (nullable = true)
 |    |    |    |    |-- url: string (nullable = true)
 |    |    |-- line: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- postalCode: string (nullable = true)
 |    |    |-- country: string (nullable = true)
 |-- maritalStatus: struct (nullable = true)
 |    |-- coding: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- system: string (nullable = true)
 |    |    |    |-- code: string (nullable = true)
 |    |    |    |-- display: string (nullable = true)
 |    |-- text: string (nullable = true)
 |-- multipleBirthBoolean: boolean (nullable = true)
 |-- communication: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- language: struct (nullable = true)
 |    |    |    |-- coding: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- system: string (nullable = true)
 |    |    |    |    |    |-- code: string (nullable = true)
 |    |    |    |    |    |-- display: string (nullable = true)
 |    |    |    |-- text: string (nullable = true)
```

## Data written by earlier releases

Earlier releases of Pathling stored resources in a different layout, which this
release does not read. That layout carries, among other differences, an
`id_versioned` column beside the resource `id`, a field identifier (`_fid`) in
every structure, a root-level `_extension` map, and decimals stored as a
fixed-point number with a `_scale` companion column. Data in that layout cannot
be upgraded in place: the change of decimal type is not one that a Parquet
dataset or a Delta table can absorb.

In this release:

- **Reading** such data through the Parquet, Delta or managed table data
  sources fails when a resource type is read, with a message naming the resource
  type, the layout detected and the remedy. The check reads only the schema
  that the read resolves anyway, so it adds no file access.
- **Writing** into such data fails in the same way when the write would add to
  it: an append to Parquet files, a Delta table or a managed table, or a merge
  into a Delta table or a managed table. The check is made for every resource
  type before anything is written, so a refused write leaves the target as it
  was, and never leaves a table holding both layouts.
  Writes that replace or skip the existing data (the `overwrite`, `error` and
  `ignore` save modes) are not refused.

The remedy is to re-import the data from its source with this release, either
into a new location or by overwriting the existing one. Where the data is a
directory of Parquet files that also holds files written by this release, the
earlier files can be removed instead.

Data is recognised as being in the earlier layout by the `id_versioned` column
at the root of its schema. If conforming data is misclassified by this check,
the check can be disabled for a single data source with its layout detection
option, as described under [Reading FHIR data](./index.md#layout-detection).
That option is intended only for conforming data that is misclassified.
