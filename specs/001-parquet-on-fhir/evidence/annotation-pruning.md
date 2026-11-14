# Whether Parquet reads only an annotation, in each placement

_Note (2026-10-26)._ The probes name the quantity's annotation `__canonical`, as the layout did
when they were run. Since decision 137 a quantity carries `__canonical_exact` only. What is read
depends on where an annotation is placed, not on its name, so the results stand for it.

Measured for decision 135, part 4, which keeps a primitive's annotations in its
`_x` group and a complex element's in its own structure, against the sibling
placement of the specification, which keeps them as `__<element>_<annotation>`
fields beside the element. Part 4 had assumed that Spark reads only the
annotation and not the rest of its container. This measurement briefly reversed
part 4 to siblings, and the owner restored it once it was clear that the
placements read the same through today's engine (decision 135).

**Result: through the engine today, the placement makes no difference to what is
read.** A FHIRPath path into a repeating element reads the whole element, whatever
field it names and wherever the annotation is: `name.family` reads all of `name`,
and `component.valueQuantity.value` all of `component`. A singular path prunes to
the field in either placement. This is
[finding 13](spark-type-findings.md#13-nested-schema-pruning-does-not-reach-the-engines-array-access)
again. The traversal filters nulls out of an array before reading a field of it,
and a higher-order function or `flatten` between two field reads hides the outer
read from Spark's schema pruning, so Spark reads the whole field that the inner
step returns.

The placements would differ only if the engine read a chain of fields before
filtering, which the second measurement below models. Even then, pairing `x`
with `_x` by naming only the annotation's field prunes (see "Alignment"), and the
container placement reads more only where `_x` must be flattened before its
field is read: a repeating primitive inside a repeating element.

## Through the engine

`scripts/EnginePruningProbe.java` encodes a Patient and an Observation from
NDJSON, stores them as Parquet, and runs a one-column view per path through the
library API, printing the `ReadSchema` of the executed plan. Measured at
`5d595dfc22`, Spark 4.0.2. The data carries no annotations, since no kind exists
yet, so this measures the elements an annotation would sit among.

| Path | Parquet reads |
| --- | --- |
| `birthDate` | `birthDate` only |
| `valueQuantity.value` | `valueQuantity.value` only |
| `code.text` | `code.text` only |
| `code.coding.code` | all of `code.coding` |
| `name.family`, `name.given`, `name.period.start` | all of `name` |
| `address.period.start` | all of `address` |
| `forEach: name`, then `family` or `period.start` | all of `name` |
| `component.code.text`, `component.code.coding.code`, `component.valueQuantity.value` | all of `component`, including the quantity's extensions |
| `forEach: component`, then `valueQuantity.value` | all of `component` |

An annotation below a repeating element is part of that element in either
placement, so it is read whenever anything below the element is.

## No setting changes it

`scripts/pruning_confs.py` (PySpark 4.0.1) reads `name.family` from an array of
structures in several shapes, under the default settings and with each of the
three settings below switched off. A shape that prunes with the defaults
reads only `name[].family`; one that does not reads all of `name`.

| Shape | Defaults |
| --- | --- |
| `name.family`, `name.period.start` | pruned |
| `filter(name.family, x -> x is not null)`, filtering after the field read | pruned |
| `filter(name, x -> x is not null).family`, the engine's shape | **whole** |
| `filter(name, x -> x.family is not null).family`, a lambda using only `family` | **whole** |
| `transform(name, x -> x.family)` | **whole** |

`spark.sql.optimizer.nestedSchemaPruning.enabled=false` turns pruning off
everywhere, and `spark.sql.optimizer.expression.nestedPruning.enabled` and
`spark.sql.parquet.enableNestedColumnVectorizedReader` change nothing. There is no
setting that turns it on through a lambda: Spark's schema pruning follows only
field extractions from a column, and any higher-order function over a whole
structure, whatever its lambda touches, makes the structure a required input in
full. Only the shape of the expression changes it.

## Why the traversal removes nulls, and where it has to

Reading a field from every element of an array gives a null for each element
that lacks it, and a FHIRPath collection has no nulls. Left in, they break the
operations that count or position items, and `flatten`, which Spark makes null
as a whole when any inner array is null (commit `07b4b6efcd`).
`scripts/pruning_late_filter.py`, on two names of which the second has neither
`family` nor `given`:

| Expression | Value | Parquet reads |
| --- | --- | --- |
| `flatten(name.given)` | null: both givens lost | `name[].given` |
| `flatten(filter(name.given, x -> x is not null))` | `[a, b]` | `name[].given` |
| `size(name.family)` | 2 | `name[].family` |
| `size(filter(name.family, x -> x is not null))` | 1 | `name[].family` |

A null element passes through a further field read as a null, so the removal is
needed only before `flatten` and before an operation that counts or positions
items, not after every step. Removed after the field read, rather than from the
array before it, it is correct and still prunes. What blocks pruning is the
filter of the array of structures before its field is read, which in conformant
data removes nothing: level 2 excludes a null item in an array of structures.

Delaying the removal in the engine, which applies equally to the layout `main`
reads, is raised as #2864.

## Through the engine's expressions, from the stored column

This measurement starts each read from the stored column, not from the filtered
array the engine's traversal produces, so its first step into an array prunes
where the engine's does not. It models an engine that filters only after a chain
of field reads.

`scripts/AnnotationPruningProbe.java` writes a two-row Parquet file holding both
placements side by side. It reads each annotation with the expressions the
engine's `DefaultRepresentation.traverse` builds:

- `ColumnFunctions.resolveOrNull` for the field;
- a `filter` that drops nulls when the input is an array;
- `flatten` where the result nests.

It prints the `ReadSchema` of the executed plan, which is what Parquet reads, and
the value, to confirm that both placements return the same thing. Run it with the
`encoders` classes and their dependencies on the classpath.

Measured at `5d595dfc22`, Spark 4.0.2, Java 21, local Parquet, with adaptive
execution off so the plan is final.

### Results

| Case | Container placement reads | Sibling placement reads | The element itself reads |
| --- | --- | --- | --- |
| Singular primitive (`birthDate`) | `_birthDate.__start` only | `__birthDate_start` only | — |
| Singular complex (`valueQuantity`) | `valueQuantity.__canonical` only | `__valueQuantity_canonical` only | `valueQuantity.value` only |
| Repeating primitive at the root (`given`) | `_given[].__start` only | `__given_start` only | `given` only |
| Primitive below a repeating element (`component.valueDateTime`) | **all of `_valueDateTime`**: `id`, `extension`, `__start`, `__end` | `__valueDateTime_start` only | `valueDateTime` only |
| Complex below a repeating element (`component.valueQuantity`) | **all of `valueQuantity`** | `__valueQuantity_canonical` only | **all of `valueQuantity`** |
| Repeating primitive below a repeating element (`name.given`) | **all of `_given`** | `__given_start` only | `given` only |

The sibling's advantage holds only one step below the first repeating element on
the path. Pruning reaches that step and stops at the filter after it, so Spark
reads whatever that step returns in full, whatever the placement and whether the
read is an annotation or the element itself. `component.valueQuantity.value`
shows it: an element one step deeper reads the whole `valueQuantity`, and so would
a sibling annotation inside it, such as `component.valueQuantity.__value_numeric`.
The same holds for `dosageInstruction.timing.repeat.boundsPeriod.start`, which in
either placement reads the whole `dosageInstruction[].timing`.

The cost of the container placement is therefore confined to annotated primitives
that are direct children of the first repeating element on their path. A quantity's canonical form below a repeating element costs
nothing extra: reading the quantity's own `value` already reads the whole
structure. Every value matched between the placements.

#### Without the filter between later steps

Reading the same paths as a plain chain of `resolveOrNull`, with the null filter
left to the end, restores full pruning for the container placement:

- `component._valueDateTime.__start` reads `component[]._valueDateTime.__start`
  only;
- `component.valueQuantity.__canonical` reads
  `component[].valueQuantity.__canonical` only.

The same change would narrow the engine's reads of ordinary elements, such as
`component.valueQuantity.value`, which today reads the whole quantity. It does
not help a repeating primitive below a repeating element. There `flatten` must
run between the steps, and `flatten` also blocks pruning: `flatten(name._given).__start`
still reads all of `_given`.

#### Alignment

A repeating primitive's annotations line up with its values by position. The
engine's traversal drops nulls, so `_given.__start` read through it returns
`[2020-01-01]` where `given` has two values, and the position is lost. Read by
position, it keeps the alignment and still prunes: `arrays_zip(given,
_given.__start)` reads `given` and `_given[].__start` only, and pairs the second
value with no annotation. The sibling `__given_start` is a separate array that
holds the same positions, `[2020-01-01, null]`.

### How much the cost would matter

- **In a fitted schema**, `_x` holds only the fields the data populates. For an
  annotated primitive without an id or extension, that is the annotations alone,
  so reading all of `_x` is close to reading the one annotation.
- **In a dense schema**, `_x.extension` carries the full extension structure,
  which [the annotation width measurement](dense-annotation-width.md) found to be
  very wide. Reading it for every annotated primitive below a repeating element
  is where the container placement would cost, under such an engine.
- Annotated primitives below a repeating element include
  `Observation.component.value[x]` as a date or a quantity's own `value`, and
  `Timing.event` below `dosageInstruction`. How many there are has not been
  counted.

## Caveats

- Two rows, local files. This measures what Spark plans to read, not time.
- Neither measurement reads an annotation through the engine, which does not read
  annotations yet. The engine measurement reads the elements beside which an
  annotation would be stored.
- Views that unnest with `transform` and `inline` read whole elements in either
  placement (finding 13). This measures FHIRPath's traversal only.
