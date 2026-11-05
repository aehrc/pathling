# Pathling and the Parquet on FHIR specification: proposed amendments and deviations

**Status:** draft for the Pathling owner's review. Once reviewed it moves to
`specs/001-parquet-on-fhir/upstream-amendments.md` and is then used to discuss the
changes with the owner of the upstream specification. Nothing in it has been
raised, commented on or committed.
**Date:** 2026-11-02, revised 2026-11-05 (sections 3, 6, 8 and 16, from the
annotations design session on `issue/2832_41-42_decimal-and-annotations`).
**Ticket:** 46 (proposed in
`.local/work/issue/2832/2026-11-02-pof-replan-proposal.md`).
**Companion evidence:** `evidence/annotations-layout-evidence.md` (draft:
`annotations-layout-evidence-draft.md` beside this file). In the revised
sections, **[M]** marks a claim that was measured, with its evidence file, and
**[I]** marks one that was inferred from documentation, code or reasoning and
not measured.

## What this document is

Pathling stores FHIR data in the layout defined by the
[Parquet on FHIR](https://github.com/aehrc/parquet-on-fhir) specification
(Pathling's issue aehrc/pathling#2367). Building that layout showed places where
the specification cannot be implemented as written, says too little, or does not
fit what an engine needs. Pathling has also made choices that depart from the
specification. This document lists every such change, in one place, so that the
Pathling owner can review them and then discuss them with the specification
owner before anything more is raised upstream.

Each change is one of:

- **amendment proposed:** a change to the specification's text that Pathling
  wants;
- **deviation in Pathling only:** something Pathling does differently from the
  specification, or cannot do, with no change to the specification asked for yet;
- **clarification:** a point the specification leaves open and Pathling has had
  to decide.

Each has one of these statuses:

- **raised upstream:** an issue exists on `aehrc/parquet-on-fhir`, and the link is
  given;
- **drafted in a ticket:** the proposal's content is written in a Pathling ticket
  or decision, but nothing exists upstream;
- **not yet drafted:** no proposal text exists. The section says what one would
  contain.

## What was read

- **Upstream specification:** `aehrc/parquet-on-fhir`, branch `main`, commit
  `c04b1617a3c7343e1db5eebdc8a23c41f7865baf` (2025-02-12, "Remove example
  script"). The repository holds `README.md`, which is the whole specification
  (719 lines), `CITATION.cff`, and three example files, `examples/Patient.parquet`,
  `Observation.parquet` and `ExplanationOfBenefit.parquet`. Cited below as "the
  README", by section name. Text:
  <https://github.com/aehrc/parquet-on-fhir/blob/c04b1617a3c7343e1db5eebdc8a23c41f7865baf/README.md>.
- **Upstream issues and pull requests, read on 2026-11-02:** there are no pull
  requests. There are three open issues, all opened by `piotrszul` (the Pathling
  owner's account), none with comments:
  - [#1](https://github.com/aehrc/parquet-on-fhir/issues/1), "canonical
    annotation: fixed-scale DECIMAL(38,6) cannot represent canonicalised values
    across their magnitude range", raised 2026-09-14;
  - [#2](https://github.com/aehrc/parquet-on-fhir/issues/2), "Identify the layout
    and its version with a required root column, `__pof_v1`", raised 2026-10-26;
  - [#3](https://github.com/aehrc/parquet-on-fhir/issues/3), "Date range
    annotations: a type that can be written (`INT64`
    `TIMESTAMP(isAdjustedToUTC=true, MICROS)`), and `instant`", raised
    2026-10-28. Its body matches the reviewed draft at
    `.local/work/issue/2832_14_date-range-annotation/pof-spec-range-type.md`
    (checked by diff: only the draft's header and trailing notes differ).
- **Pathling:** `specs/001-parquet-on-fhir/` (`spec.md`, `evidence/decisions.md`,
  `contracts/storage-layout.md`, `data-model.md`, `issues/`), `docs/adr/`,
  `CONTEXT.md`, and the code on `issue/2832` at `9e3d2f46b3` where a claim depends
  on it. Paths below are relative to `specs/001-parquet-on-fhir/` unless they
  start with `docs/` or `.local/`.

## Three amendments are already raised

Three of the changes below are already open upstream: #1 (the canonical
annotation, section 8), #2 (the layout column, section 1) and #3 (the range
annotation type and `instant`, sections 6 and 7). The specification owner has
seen them, and has not yet responded. Neither #1 nor #2 is recorded in any Pathling
ticket. Ticket 30, whose purpose is the layout column proposal, still has its
"raised" checkbox unticked and no issue number, and the re-plan of 2026-11-02
assumes nothing further is raised before this review. Ticket 14 records #3.
`data-model.md` says only "Raised upstream" for the canonical annotation.
Everything else in this document is not yet upstream.

## Summary

| # | Change | Kind | Status |
|---|---|---|---|
| 1 | A required root layout column, `__pof_v1`, and versioning | amendment proposed | raised upstream ([#2](https://github.com/aehrc/parquet-on-fhir/issues/2)) |
| 2 | Every field whose name begins `__` is layout, not FHIR content | amendment proposed | drafted in a ticket (35, item 2) |
| 3 | Where annotations are stored (four placements) and whether they are flat or grouped, set out without a preference; the Quantity example is corrected | open question (placement, grouping); amendment proposed (Quantity example) | drafted here, 2026-11-05 (replaces ticket 35, item 4) |
| 4 | The JSON projection, conformance level 2 and the round-trip invariant | amendment proposed | drafted in a ticket (35, item 1) |
| 5 | `resourceType` is in every row, and what "required" means for a Spark writer | clarification | drafted in a ticket (35, item 3) |
| 6 | Range annotations: a physical type that can be written, microsecond unit (not milliseconds, not nanoseconds), proleptic Gregorian calendar (with INT96 as the alternative) | amendment proposed | raised upstream ([#3](https://github.com/aehrc/parquet-on-fhir/issues/3)); the INT96 revision (ticket 44) is not yet drafted; INT96 cost and the NANOS range added here, 2026-11-05 |
| 7 | `instant` carries range annotations | amendment proposed | raised upstream ([#3](https://github.com/aehrc/parquet-on-fhir/issues/3), item 3) |
| 8 | The canonical annotation's value type, one stored representation per magnitude, and order for row-group skipping | amendment proposed | raised upstream ([#1](https://github.com/aehrc/parquet-on-fhir/issues/1)); the uniqueness requirement and the order consequence drafted here, 2026-11-05 |
| 9 | Pathling emits `__canonical_exact` in place of `__canonical`, and the canonicalisation rules the specification leaves open | deviation in Pathling only | not yet drafted (the rules); the substitution is recorded in decision 137 |
| 10 | What the `numeric` annotation holds beyond six fractional digits and past its range | clarification | not yet drafted |
| 11 | A decimal's text is numerically equal to the source, not its lexical form | deviation in Pathling only | not yet drafted (no upstream change is needed; tracked as OQ-1 and ticket 16) |
| 12 | Primitive ids and extensions: repeating primitives, positional nulls, `_x` without `x`, and the specification's own example | amendment proposed | not yet drafted (ticket 17 is a placeholder) |
| 13 | Elements of the abstract `Resource` type, `contained` first, are not represented | deviation in Pathling only | not yet drafted |
| 14 | `unsignedInt` and `positiveInt` are stored as signed `INT32` | deviation in Pathling only | not yet drafted (unrecorded in Pathling's deviation table) |
| 15 | Fields the definitions do not describe, and their place in the conformance levels | clarification | not yet drafted (ticket 36 scopes a Pathling issue) |
| 16 | What an annotation means when it is null, when tables are merged, and who may trust it | clarification | a derived annotation SHOULD be non-null exactly where its value is: drafted here, 2026-11-05; the rest not yet drafted (tickets 38 and 26) |
| 17 | The range of a value that states a time but no timezone | clarification | not yet drafted (ticket 26) |

Dependencies between the changes, in short. Every placement in 3 needs 2, and
options 1 and 4 need 4, because the projection must leave out `__` fields
inside structures. Option 1 also needs 12, because a primitive's annotations
share its metadata group. 6 and 7 as raised use the specification's present
placement (`__birthDate_start`, option 3), so they need rewording unless option
3 is chosen. 8's ordered-type consequence and 6's type rest on the same
row-group skipping evidence. 8 and 9 overlap and should be settled as
one. 4 cannot be claimed by Pathling until 11, 13 and the base64 whitespace
exception are closed, or listed as exceptions.

---

## 1. A required root layout column, `__pof_v1`

**Kind:** amendment proposed. **Status:** raised upstream,
[aehrc/parquet-on-fhir#2](https://github.com/aehrc/parquet-on-fhir/issues/2).

### Context

**What the specification says.** Nothing identifies a table as following the
specification, or says which version. The README has no version number (#2 asks for
`CITATION.cff` to gain `version: "1.0"`). "Field optionality" requires only `resourceType`:
"The only field that must be present within the table is the `resourceType`
field", and "Consuming applications SHALL be able to tolerate the absence of any
field within the schema except for `resourceType`."

**What Pathling built.** Pathling's engine reads only the new layout, and
refuses data in its earlier layout (ADR-0001, decision 132). To tell the two
apart exactly, decision 132 and ADR-0006
(`docs/adr/0006-layout-identified-by-a-versioned-column.md`) make every dataset
that ingest produces carry a root column, `__layout_v<n>`: a nullable boolean,
every value null, recognised by name and type alone (FR-065, FR-066). The version
is in the name because reading a value would take a Spark job per table and an
empty table has none. Footer key-value metadata, Spark field metadata, Delta
table properties and a sidecar file were rejected (ADR-0006, "Considered
options"). **No code writes the column yet:** ticket 29, which builds it, is
unstarted, and ticket 30 asked for the proposal so that Pathling takes the name
and the version rule from the specification. `__layout_v1` is a placeholder.

### Issue

Without a layout column a reader cannot tell this layout from another FHIR
encoding that uses the same names, or one version from the next, so once a field's
type changes an older reader returns wrong answers rather than an error. This is
a problem for any consumer. For Pathling it decided how the legacy layout is
refused. The specification's own example files carry no such column (see 5), so
every table written to date is unmarked.

### Proposal

As raised in #2: an `optional boolean __pof_v<n>` at the root of every table,
every value null, where `<n>` is the specification's major version. A reader
recognises it by name and type alone and refuses a table whose column is missing,
duplicated or of an unsupported version. A producer refuses to append data to a
table if either lacks the column or the major versions differ. The
specification adopts semantic versioning, released as 1.0, and raises the major
version only for a change that would make a reader of the previous major version
misread complying data. Replacement text for "Field optionality", a new "Layout
column" section, a reserved-name sentence in "Annotations", and regenerated
example files are in the issue.

Alternatives considered in the issue: the version as a separate layout counter
that the specification maps to its releases (same name, same behaviour) if the
maintainers do not want semantic versioning. Alternatives Pathling considered
and rejected are in ADR-0006 and are the same as in the issue's reasons.

### Other considerations

- **Name.** Pathling's FR-065, FR-066, ADR-0006 and `CONTEXT.md` still say
  `__layout_v1`; #2 proposes `__pof_v1`. They are expected to follow the agreed
  name (ticket 30's third criterion). Nothing in code refers to either name, so
  the rename is free until ticket 29 lands.
- **Contradiction: unmarked data.** #2 says a reader SHALL refuse a table with no
  layout column and a producer SHALL refuse to append to one. Pathling does the
  opposite on purpose: "a schema carrying no marker field MUST be accepted"
  (FR-039), and unmarked PoF data is "its regression test" (ticket 29, "Out of
  scope"). ADR-0006 says merging marked with unmarked data "widens the schema as
  any other column does" and that a table with no column "is classified by
  markers, and accepted". If #2 is agreed as written, FR-039, FR-066, ADR-0006
  and ticket 29 change, and so does the status of every table Pathling and its
  predecessors have written, plus the specification's own example files (#2 asks
  for them to be regenerated). The owner should decide whether Pathling asks for a
  transition rule (a reader MAY accept an unmarked table) or accepts the strict
  one.
- **Contradiction: the version rule.** #2 raises `<n>` only when a reader of the
  old version would misread data, and calls a change that breaks only producers a
  minor release. Ticket 29 says "If ticket 16 later changes what is stored, it
  bumps the version", and its comment expects a version bump for ticket 16's
  lexical decimals. Under #2's rule a change from "numerically equal text" to the
  lexical form misleads no reader, so it would not bump the version. Pathling's
  plan and the proposal disagree about when the version moves.
- **Contradiction: duplicates.** #2 refuses a table with two layout columns.
  Pathling refuses a layout column together with the legacy root `id_versioned`
  as "mixed" (FR-066), which #2 does not address and does not need to.
- **What Pathling writes versus what #2 requires.** FR-065 requires the column
  only on datasets that ingest produces from JSON or XML. #2 requires it of every
  conforming table, including one an annotation step or a view produces, and says
  a transformation that leaves it out produces a non-conforming table. Pathling
  has not decided what its other outputs do.
- **Evidence.** #2's reasons are stated as facts. Only one is measured in the
  repository: Spark refuses to write a null-typed (`void`) column to Parquet
  (`evidence/spark-type-findings.md`, script `evidence/scripts/nulltype_test.py`).
  The claims that footer metadata cannot be set by Spark's DataFrame writer, that
  an all-null boolean survives Parquet and Delta writes, overwrites, appends and
  merges, and that field metadata is dropped by some engines were described from
  memory (ticket 29, "Probes first"). `evidence/layout-column.md`, where ticket
  29 says they are recorded, does not exist. Run them before relying on them in
  the discussion.
- **Dependence.** #2 calls the reservation of every `__` field a "companion
  proposal" (section 2). With the container placement (section 3) the layout
  column is the only root-level `__` field.
- **If rejected.** Pathling keeps the column as its own non-standard root field
  (name and version rule as it chooses), which is valid under the specification's
  rule that non-standard `__` names may be used if they do not collide. ADR-0006
  would then say so, and other producers would not write it, so Pathling could not
  tell their data from unmarked legacy-shaped data except by markers. The
  layout-versioning guarantee would be Pathling-only.

---

## 2. Every field whose name begins with `__` is layout, not FHIR content

**Kind:** amendment proposed. **Status:** drafted in a ticket
(`issues/35-pof-spec-json-projection.md`, item 2; the proposal itself is not
written).

### Context

**What the specification says.** "Annotations" says an annotated field is
`__[element]_[annotation name]` and that "Non-standard annotations can be
included in the format provided that their names do not collide with the
annotations defined in this specification." It does not say that `__` is
reserved, that such a field is outside the FHIR content, or that a conversion to
FHIR leaves it out. Ticket 35 asked how far the `__<field>_canonical` naming
already says this: the answer is that it says only that annotation names have
that shape.

**What Pathling built.** A field whose name begins with `__` is layout, not
content, and so are the fields Pathling does not define: egress leaves out every
`__` field (decision 135, part 3), conformance level 3 for Parquet compares the
schema with the layout the definitions derive "apart from any field whose name
starts with `__`" (`evidence/ingress-strictness/README.md`, "Conformance
levels"), and a `__` field Pathling does not define is kept by default
(`dropUnknownAnnotations` is off, decision 135's addendum of 2026-10-23).

### Issue

A converter to FHIR cannot know which extra fields to drop. Without the rule, a
foreign annotation, a layout column and Pathling's own annotations would each
need to be listed by name, and a reader cannot tell a future annotation from
content the definitions do not describe. FHIR element names never begin with an
underscore except a primitive's `_x` group, so `__` is free.

### Proposal

Add to "Annotations": a field at any level of a table whose name begins with
`__` is a layout field. It is not FHIR content, a conversion to a FHIR format
SHALL leave it out, and a consumer SHALL ignore one it does not know. Producers
may add layout fields beyond those the specification defines. The layout column
(section 1) and the annotations are instances.

Alternative: list the annotation names the specification defines and treat any
other `__` field as unknown content. This is the status quo.

### Other considerations

- **Compatibility.** Nothing the specification defines today begins with `__`
  except annotations, so no conformant table changes meaning. A producer that
  already writes a non-standard `__` field (the specification permits it) gains
  the rule that it is left out of FHIR output.
- **Dependence.** Needed by sections 1, 3 and 4. 3 puts `__` fields inside
  structures where FHIR has none, which only works if converters and level 3
  ignore them.
- **Open question for the specification owner.** Is a consumer's obligation to
  *ignore* an unknown `__` field enough, or must it also preserve it when
  rewriting a table? Pathling's annotation step keeps unknown ones by default and
  can drop them (`dropUnknownAnnotations`), which is its answer, not a
  specification rule.
- **If rejected.** Pathling keeps the rule for itself (egress and level 3 already
  apply it), and documents it as a Pathling convention. The cost is that another
  implementation's egress may fail on or emit Pathling's annotations.

---

## 3. Where annotations are stored, and whether they are flat or grouped

**Kind:** open question for the specification, with four placements and two
groupings set out side by side. Pathling proposes none of them. **Status:** not
yet raised. Revised 2026-11-05. This replaces ticket 35 item 4, which proposed the
placement Pathling built (decision 135, part 4), as the text to discuss.
Evidence: [`evidence/annotations-layout-evidence.md`](evidence/annotations-layout-evidence.md)
and the files it links.

### Context

**What the specification says.** "Annotations": an annotated field is a sibling
of its element, named `__<element>_<annotation>`: `__birthDate_start`,
`__valueDecimal_numeric`, and for a quantity a group `__valueQuantity_canonical`.
"Primitive extensions and internal identifiers" gives a primitive's `_x` group the
fields `id` and `extension` only. The specification says nothing about
annotating a repeating element, where a sibling `__x_start` would have to be a
second array aligned with `x`. Its Quantity example is internally
inconsistent: the "Quantity unit canonicalization" section lists the group's
fields as `value` (a decimal) and `code`, but the Observation example's
`__valueQuantity_canonical` group repeats the whole Quantity shape (`code`,
`system`, `unit`, `value` and `__value_numeric`). The upstream example files use
the sibling placement for `numeric`: `Patient.parquet` has
`__valueDecimal_numeric` beside `valueDecimal`, and `Observation.parquet` has
`__value_numeric` inside `valueQuantity`. They carry no range annotation and no
canonical group.

**What Pathling built.** Option 1 below (decision 135, part 4, owner,
2026-10-22): a primitive's annotations are fields of its `_x` group, after `id`
and `extension` (`_birthDate.__start`, `valueQuantity._value.__numeric`), and a
complex element's are fields of its own structure (`valueQuantity.__canonical_exact`).
The decimal (ticket 13), date range (ticket 14) and quantity (ticket 12)
annotations are built this way on `issue/2832` [M]. No Pathling release writes
annotations yet, so nothing is published in any placement.

### Issue

The specification does not say how to annotate a repeating element, and it
leaves two choices open that every producer and consumer depends on:

1. **Placement:** where an annotation is stored relative to its value.
2. **Grouping:** whether an element's annotations are separate scalar fields or
   one structure.

Separately, the Quantity example contradicts its own table.

### The four placements

Three elements show each placement: a singular date (`Patient.birthDate`), a
repeating date inside a repeating element
(`MedicationRequest.dosageInstruction[].timing.event[]`), and a quantity
(`Observation.valueQuantity`). Field names in options 2 and 3 are illustrative.

| Option | `birthDate` | `dosageInstruction[].timing.event[]` | `valueQuantity` |
|---|---|---|---|
| 1. Inside the primitive's metadata group `_x` (Pathling today) | `birthDate`; `_birthDate {id, extension, __start, __end}` | `event[]`; `_event[] {id, extension, __start, __end}` | `valueQuantity {value, unit, …, __canonical…}` |
| 2. A sibling annotation group `__x` | `birthDate`; `_birthDate {id, extension}`; `__birthDate {start, end}` | `event[]`; `_event[] {id, extension}`; `__event[] {start, end}` | `valueQuantity {…}`; `__valueQuantity {canonical…}` |
| 3. Separate sibling fields at the parent's level (the specification today) | `birthDate`; `__birthDate_start`; `__birthDate_end` | `event[]`; `__event_start[]`; `__event_end[]` | `valueQuantity {…}`; `__valueQuantity_canonical…` |
| 4. Zipped: every primitive is one structure | `birthDate {value, id, extension, __start, __end}` | `event[] {value, id, extension, __start, __end}` | as option 1 |

Options 1 to 3 keep the value `x` a plain column. Option 4 changes only
primitives, because a complex element is a structure already.

#### Format facts

These hold for the files, whatever reads them.

- **Selective reads.** Parquet stores every leaf as its own column chunk, so in
  all four options a reader can in principle read a value without its
  annotations, and an annotation without the extensions [I]. Zipped and option 1
  files are the same size to within 4%: −0.05% with heavy extensions, +3.7% with
  dates only, and the value leaf is byte-identical [M, zipped probe]. Options 1 to
  3 were not compared with each other.
- **Statistics and row-group skipping.** Leaf statistics are kept at any depth.
  A flat and a grouped annotation skip the same row groups [M, pushdown, arm 6].
  Under a repeated field, statistics support only a superset test [I].
- **Alignment.** Option 1: a repeating value and its annotations are aligned by
  position, the same rule FHIR JSON already uses for ids and extensions.
  Option 2: a second positional array beside `_x`. Option 3: one positional array
  per annotation. Option 4: the alignment is structural, with nothing to keep in
  step. In options 1 to 3, a whole-null metadata array has to mean "no metadata
  for any item" [M, pairing probe].
- **What SQL equality sees.** In options 1 to 3 the value column holds only the
  value, so plain SQL equality, `DISTINCT`, `UNION` and `array_contains` compare
  values. In option 4 they compare the value together with its id, extensions and
  annotations. On equal values with different extensions, the following were
  measured on Spark 4.3.0, Spark 4.0.2 and DuckDB 2.0 [M, zipped probe]:
  - `SELECT DISTINCT given` returns 4 rows against 2;
  - `array_distinct` returns 2 against 1;
  - `array_contains(given, 'Ann')` does not type-check;
  - a self-join on the value finds 1 pair against 6.

  A consumer of option 4 has to project `.value` wherever it compares or
  returns a value.
- **Fidelity to FHIR JSON and to the legacy layout.** Option 1 keeps FHIR JSON's
  `x` and `_x` shape and adds `__` fields to `_x`, which a conversion to FHIR
  leaves out (sections 2 and 4). Option 2 leaves `_x` as FHIR defines it.
  Option 3 is the specification today. Option 4 departs from FHIR JSON's shape
  and from the legacy (Bunsen) layout, in which a primitive is a plain value.
- **How the schema changes when an annotation is added.** Option 1 adds fields
  to `_x`, creating `_x` where there was none. Option 2 adds a field or a column
  `__x`. Option 3 adds a column per annotation. In option 4 every primitive is a
  structure even without metadata, so adding an annotation adds a field but
  never changes a plain value into a structure [I].
- **Schema discovery.** In every option a SQL generator has to read the schema
  first, because DuckDB and Trino refuse to bind a field that is absent [I].
  Option 3 has the most names to discover.
- **Complex elements.** In options 1 and 4 a quantity's canonical form is inside
  the element, so it stays with the element through filters, unions and arrays.
  In options 2 and 3 it is beside the element, and a consumer that keeps the
  element alone loses it [I].

#### Tool information

These are what today's engines do. They count only through adoption (see
"Other considerations"), and they change as engines change.

- **Value-only reads in Spark.** Options 1 to 3 prune to the value leaf at every
  depth on Spark 4.0.2 and 4.3.0 [M]. Option 4 reads the whole element for a
  repeating primitive below a repeating parent [M, zipped probe, Spark 4.3.0]:
  - `name[].given[]`: 47.6 MB against 0.6 MB, 79 times;
  - `dosageInstruction[].timing.event[]`: 29 times;
  - with dates only and no extensions: 2 and 3.5 times.

  Singular and one-level reads are identical in both layouts [M].
- **Annotation reads on a repeating primitive in Spark.**
  - One level down: `arrays_zip(event, _event.__start, _event.__end)` reads only
    those three leaves on 4.0.2 and 4.3.0 [M, pairing probe].
  - Below a repeating parent, Spark 4.3.0 reads the whole of `_event`, extensions
    included: 321,789 bytes against 19,488, about 16 times [M]. The cause is
    Spark's pruning through two nested array levels, not the zip: a control with
    no zip reads the same [M].
  - Option 4 reads the whole structure there (39.6 MB against option 1's 38.3 MB)
    [M].
  - Options 2 and 3, Spark 4.3.0 with no rule: no extensions are read.
    - The sibling group reads 22,452 bytes.
    - The parent-level fields read 22,398 bytes, exactly the annotations used.

    [M, `evidence/annotations-layout-assessment.md`, section 5]
  - On Spark 4.0.2, every placement reads the whole repeating parent [M].
- **Other engines.** DuckDB 2.0 prunes no layout below a list: it reads the
  top-level column. It prunes a singular structure to its leaf [M]. Polars
  prunes top-level columns only [M].
- **Pairing positional arrays.**
  - A positional zip that keeps nulls exists in Spark (`arrays_zip`), DuckDB
    (`list_zip`) and Trino (`zip`). Polars has no single operation and needs an
    explode and re-implode idiom, which was not probed [I, documentation].
  - Spark's `arrays_zip` returns null when any input is null, so the inputs need
    a guard [M].
  - Option 4 needs no pairing.
- **Pathling.**
  - Option 1 is built [M].
  - For options 2 and 3, Pathling already tolerates absent root columns. Reading
    a sibling field with a fallback takes about 11 lines over existing code
    [M, quantity-root, part B].
  - For option 4, Pathling would map the stored structure to its element form.
    On current Spark, its benchmark views that read `name.given` would read more
    than today [I, from the zipped probe].

### Flat or grouped annotations

**Flat:** one scalar field per annotation (`__start`, `__end`;
`__canonical_code`, `__canonical_value_exact`). **Grouped:** one structure per
element (`__bounds {start, end}`, `__canonical {value, code}`).

- **Format.**
  - Row-group skipping is identical [M, pushdown, arm 6].
  - A group guarantees that its fields appear in the schema together. It does
    not guarantee they are filled together in a row, because the fields stay
    nullable [I].
  - Flat matches the specification's names `start` and `end`, and the legacy
    layout's flat quantity fields (`_code_canonicalized`, `_value_canonicalized`).
  - Grouped adds a level to every path.
- **Pathling.**
  - Built so far: date ranges flat; quantity grouped (`__canonical_exact`,
    decision 137).
  - Pathling's current preference is flat (owner, 2026-11-04), including the
    quantity (`design/annotations-in-the-engine.md` §4.7, not yet adopted). This
    is a preference, not a proposal.

### Proposal

None of the placements or groupings is proposed. Pathling asks the specification
owner to choose one of each. Pathling then drafts replacement text for
"Annotations", the examples and the three published example files.

Independently of that choice, Pathling proposes correcting the Quantity example
and its table so that they agree.

### Other considerations

- **How Pathling judges the options.** This is Pathling's own position, not
  proposed as specification process. A layout should be shown to work for two or
  three implementations before the specification is released. At the proposal
  stage, a check of how each could work is enough. That check is done for all
  four placements on Spark, DuckDB, Trino and Polars, and none is blocked
  (companion evidence, "Conceptual validation") [I]. Measurements become
  pre-release evidence once real implementations exist.
- **Superseded evidence.** `evidence/annotation-pruning.md` found that placement
  did not change what Pathling's engine read. That holds for the engine as it
  was, because the engine then read whole repeating elements in every placement.
  The probes above measure Spark directly, and they do show differences.
- **Contradiction with #3.** This is ticket 46's finding "#3 against decision
  135". #3's text and example schema use option 3's names (`__birthDate_start`).
  Under option 3, #3 stands as written for placement and Pathling moves. Under
  the other options, #3's text is reworded to the chosen names. The type (#3)
  and the placement can be agreed in either order.
- **Overlap with #1.** Ticket 35 item 4 also offered `__canonical_exact` in place
  of the specification's fixed-point form. The type belongs to section 8, and
  this section carries only the placement and grouping.
- **A stored shape the specification does not mention, under option 1.** A
  primitive carried only for an annotation has a `_x` group with no `id` or
  `extension` (decision 135, part 4).
- **Dependence.** Sections 2 (ignore `__` fields), 4 (the projection leaves out
  annotations) and 12 (positional alignment of `_x` for a repeating primitive).
- **What Pathling changes under each option.**
  - Option 1: nothing.
  - Options 2 and 3: the writer and the annotation readers move to the new
    names, and the quantity's canonical form needs its parent through every
    operation (decision 135). Tickets 11 to 14 and 41 are reworked.
  - Option 4: the stored form departs from FHIR JSON, so the JSON projection
    (ticket 35) and the legacy export change. The engine maps storage to its
    element form, and value reads of nested repeating primitives regress on
    current Spark [M, zipped probe].

---

## 4. The JSON projection, conformance level 2 and the round-trip invariant

**Kind:** amendment proposed. **Status:** drafted in a ticket
(`issues/35-pof-spec-json-projection.md`, item 1; no proposal text is written).

### Context

**What the specification says.** The introduction says the format "is intended
to be a lossless representation of FHIR. For example, FHIR JSON converted to this
format should be able to be regenerated from the format without any loss of
data." It defines no mapping back to JSON, and says nothing about nulls in
arrays, empty structures, or a field that is null in a row. "Repeating elements"
and "Field optionality" imply that absent means null.

**What Pathling built.** Pathling defines conformance in levels
(`evidence/ingress-strictness/README.md`, "Conformance levels"): 1 the container
format, 2 FHIR's JSON encoding rules, 3 the definitions' names, types and
cardinality, 4 value lexical spaces, 5 valid FHIR. Level 2 is defined "by
reference to FHIR's JSON rules, not restated"; for a PoF row it is applied to
the row's JSON projection: a field starting with `__` is left out (section 2), a
null field in a structure is omitted, a null in an array is written as `null`,
and `resourceType` is at the root (section 5). The round-trip invariant (owner,
2026-10-22): for FHIR conforming to a given definition set, JSON to PoF to JSON
is an identity up to JSON equivalence, ignoring key order and formatting, with the
narrative equal under HAPI's `equalsDeep`. FR-016 states it with exceptions.
Pathling's own egress does not yet meet the projection: rules 2 and 3 of
`data-model.md`, "Round-trip rules", an empty object and an all-null array, are
deferred, because the placeholders of decisions 71 and 72 write `{}` for a
structure holding only a primitive's metadata, and a positional null without the
`_x` array it aligns with, until ticket 17.

### Issue

The specification asserts losslessness but defines neither the projection nor
what a lossless table is. Without it, two producers can disagree on whether `[]`,
an empty structure, `""` or a positional null with no `_x` are valid content,
and a validator cannot decide a table's conformance without restating FHIR's JSON
rules.

### Proposal

As drafted in ticket 35 item 1: define the projection (a null field in a
structure is omitted, a null in an array is written as `null`, `__` fields are
left out, `resourceType` is at the root) and say a table is conformant at level 2
when its projection conforms to FHIR JSON. That excludes `[]`, a structure whose
every field is null, a null item in an array of structures, `""` and a
positional null with no aligned `_x`, without the specification restating them.
A value's JSON type, such as a decimal stored as text written as a number, comes
from the definitions, not the projection. State the round-trip invariant: JSON to
PoF to JSON is an identity up to JSON equivalence for FHIR that conforms to the
definitions.

Alternatives: list Pathling's own level 2 rules in the specification (what
Pathling would do without this change), or keep "lossless" as an intent without a
mapping.

### Other considerations

- **The invariant has exceptions of Pathling's own.** FR-016 lists: `contained`
  and inline resources in elements typed `Resource` (section 13), a decimal's
  lexical form (section 11), `base64Binary` whitespace, and primitive ids and
  extensions "until ticket 17". Ticket 35 proposes upstream an invariant that
  Pathling itself does not meet. Either the proposal carries those exceptions as
  the specification's, or Pathling closes them first. `base64Binary` is binary in
  Pathling, as the specification requires, so whitespace FHIR permits inside the
  text is not kept. The owner should decide which.
- **Levels 3 to 5 are Pathling's.** The specification defines no levels. Whether
  the specification owner wants conformance levels at all, or only the
  projection and the invariant, is open.
- **Evidence.** The corpus for the round-trip tests is the 2,911 example
  resources FHIR publishes (decision 63): 81 carry primitive metadata and none a
  null in a value array. That is FHIR's example set, not the Parquet on FHIR one.
- **Compatibility.** The upstream example files cannot be checked against level
  2 without a projection to run. A table written by another producer that stores
  `[]` or an empty struct would become non-conformant.
- **Dependence.** Needs section 2 (the rule that `__` fields are left out) and
  section 5. Section 12 closes the positional-null case that level 2 excludes.
- **If rejected.** Pathling keeps its levels as its own, defined in
  `evidence/ingress-strictness/README.md`, and states the invariant for its own
  datasets only. Nothing in the engine changes. The follow-up work in ticket 35's
  "Not here" list (egress as the projection, the `resourceType` column, FR-019's
  pruning) is Pathling's either way.

---

## 5. `resourceType` is in every row

**Kind:** clarification. **Status:** drafted in a ticket
(`issues/35-pof-spec-json-projection.md`, item 3).

### Context

**What the specification says.** "Resource type": the schema SHALL contain a
field named `resourceType` of type `binary` with the `STRING` logical type, "set
to a type from the FHIR resource types value set", and all rows SHALL hold the
same value. "Field optionality": it "MUST always be in the schema ... and also
marked as required within the schema", and "All fields in the schema other than
the `resourceType` field SHALL be marked as optional."

**What Pathling built.** `resourceType` is the first field of every resource
table, and ingest writes it in every row: the source's own value if the source
carries one, otherwise the type it was told to read (`ResourceTransformer`;
`LayoutFields`, "resourceType is the first field of a resource"). Spark has no
way for Pathling to mark it `required`; see below.

### Issue

Ticket 35 reads the text as already requiring a value in every row, and asks that
the rule be explicit so that "the row carries everything the projection writes"
(section 4). Two things are not covered. First, "marked as required" cannot be
met by a Spark writer: all three upstream example files, written by parquet-mr
1.13.1 (Spark), declare `optional binary resourceType (String)`, and many of
their nested groups are `required` where the specification says every other
field is `optional`. Spark's documentation says it converts every column to
nullable when writing Parquet. I read the example files' schemas with pyarrow;
I did **not** run a Spark write to confirm what Pathling's own output declares.
Second, an `optional` field allows a null value in a row, which the text does not
rule out.

### Proposal

State in "Resource type" that every row's value SHALL be non-null and equal to
the table's type. Either keep "required" with a note that a producer that cannot
write required fields (Spark) SHALL still write a non-null value in every row, or
drop "required" and rely on the value rule. The README's own history shows the
tag was added to the examples later (commit `8a1363d`, 2024-08-28, "Add required
tag to resourceType within examples") without the example files following.

### Other considerations

- **Contradiction in ticket 35.** Its "Not here" list names "the `resourceType`
  column" as a Pathling change to follow, but the transformer already writes it in
  every row. What would change is how an input row of a different type is handled
  (decision 70, "One resource type per input": such a row keeps its own `resourceType` string), and the
  `required` marking.
- **Unverified.** Whether Pathling's written files declare `resourceType`
  `required` or `optional` was not measured. A probe write was refused in this
  session. A deviation of this kind would not be in `contracts/storage-layout.md`,
  whose table lists no such item.
- **If rejected.** Nothing in Pathling changes. If the specification keeps
  "required" as a hard rule, Pathling and every Spark writer deviate, and Pathling
  would have to record it.

---

## 6. Range annotations: a physical type that can be written, microseconds, and the calendar

**Kind:** amendment proposed. **Status:** raised upstream,
[aehrc/parquet-on-fhir#3](https://github.com/aehrc/parquet-on-fhir/issues/3);
the revision that ticket 44 asks for is not yet drafted.

### Context

**What the specification says.** "Date ranges": "Both the `start` and `end`
annotations use the `int96` Parquet primitive type, and the
`TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS)` logical type", with the example
`optional int96 __birthDate_start (TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS))`
and a value of `2014-06-01T12:05Z` having an end of `2014-06-01T12:05:59.999Z`.

**What Pathling built.** Parquet allows no logical type on `int96`, so that
combination cannot be written, and Pathling stores `start` and `end` as Spark's
`TimestampType`, which is UTC-adjusted at microseconds (ticket 14, comment 2;
probe `evidence/scripts/ticket-14/range-type-probe.txt`, Spark 4.0.2, plain
Parquet and Delta). Decision 138 (owner, 2026-10-27, amended 2026-10-28): Pathling's
own sessions set `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS`, so the
bounds are `int64 (TIMESTAMP(MICROS,true))` on the proleptic Gregorian calendar,
from `java.time.Instant` so a date before the Gregorian cutover is not rebased. A
session the user brings keeps its own setting (`INT96` by default). The code does
both things decision 138 describes: it logs a warning at context creation
(`PathlingContext`), and the Parquet, Delta and catalog sinks throw
`IllegalStateException` before writing any dataset that holds a timestamp, which
means a range annotation (`StoredTimestamps`). `contracts/storage-layout.md`
records the deviation, lasting until the specification is amended.

### Issue

1. The specified type cannot be written. A writer can meet the physical type or the
   logical type, not both. Spark writes plain `int96` by default, or `int64
   (TIMESTAMP(MILLIS,true))` or `int64 (TIMESTAMP(MICROS,true))` if the session
   says so, and Delta the same.
2. The unit decides what "latest" means. At milliseconds the stored end of
   `1980` is `1980-12-31T23:59:59.999Z`, the engine's computed end for the
   literal `@1980` is `1980-12-31T23:59:59.999999Z`, and the equal-ranges branch
   of a comparison stops holding. The probe shows `birthDate >= @1980` for a
   stored `1980` going from true to empty at MILLIS (and
   `birthDate <= @1500-02-03` for a stored `1500-02-03`).
3. The calendar for dates before 1582-10-15 is not named.

### Proposal

As raised in #3: `int64` with `TIMESTAMP(isAdjustedToUTC=true, unit=MICROS)`,
values on the proleptic Gregorian calendar, and the example value reworded to
`2014-06-01T12:05:00.000000Z` and `2014-06-01T12:05:59.999999Z`. The issue gives
replacement text and example schemas. Item 3 of the issue, `instant`, is section
7.

Alternative being weighed (ticket 44, owner, 2026-10-29, raised from the review of
PR #2877, finding 3): keep `int96`, the specification's physical type and Spark's
default, as an allowed form beside `int64`, so a session the user brings (a
Databricks notebook's, or one passed to `PathlingContext.create(spark)`) can write
as it is. Under decision 138 such a session fails every write that carries
annotations until the user changes a Spark setting. Ticket 44 asks for a revision
of #3, as a comment, "with the costs of each type and a recommendation".

### Other considerations

- **INT96 cost, now measured** (revised 2026-11-05). Decision 138 rejected
  `INT96` because it "never gets statistics or predicate pushdown". This is now
  measured [M, `evidence/annotations-design-pushdown.md`, result 5]:
  - `__end` stored as `INT96` has no footer statistics, so all 262 row groups
    are scanned.
  - `PushedFilters` lists the predicate exactly as it does for
    `TIMESTAMP_MICROS`, so the plan alone does not reveal the difference.
  - The same filter on `int64 TIMESTAMP(MICROS)` skips the row groups whose
    footer maximum is below the bound.

  `range-type-probe.txt` shows that the comparison results are identical for
  `INT96` and `int64` microseconds [M]. The footers show `INT96` with no logical
  type, so a schema does not say the value is UTC-adjusted [M]. I also recall,
  unverified, that older Hive and Impala have written `INT96` as local time,
  which would make "an instant" ambiguous across producers. Check before it is
  said to the specification owner.
- **The pushdown benefit is not realised today, but the shape that realises it
  is known** (revised 2026-11-05). Ticket 26 records that today's date comparison
  is a `CASE WHEN` inside a lambda, which Spark does not push down. The pushdown
  probe shows two things [M, results 3 and 4]:
  - that shape skips nothing;
  - adding a plain condition that every true match satisfies, such as
    `_birthDate.__end >= 2000-01-01T00:00Z` beside the comparison, skips row
    groups without changing the answer.

  Pathling's engine design adopts that shape (decision D11, 2026-11-03), and it
  is not yet built [I]. It needs an ordered type with statistics, which
  `int64` microseconds is and `INT96` is not.
- **Why microseconds and not nanoseconds: the range** (added 2026-11-05;
  answers ticket 46's finding "decision 138 unmeasured"). A signed 64-bit count
  covers ±2^63 units from 1970 [I, arithmetic]:
  - in nanoseconds, only 1677-09-21T00:12:43Z to 2262-04-11T23:47:16Z;
  - in microseconds, about ±292,277 years.

  A FHIR `date`, `dateTime` or `instant` may have any year from 0001 to 9999. So
  `int64 TIMESTAMP(NANOS)` cannot hold the range of `1600` or `2300`, while
  MICROS holds every FHIR value. This is a format ground, independent of any
  engine. MILLIS also covers the range, and is ruled out by the unit argument
  under "Issue", point 2. Decision 138 records MICROS against MILLIS but not
  against NANOS, so a line recording this range is to be added there.
  Separately, and as tool information only: pandas' default `datetime64[ns]`
  has the same 1677–2262 limit [I].
- **Contradiction: what a reader does with another type.** #3's compatibility
  paragraph says "a reader that does not find the type it expects computes the
  range from the element". Pathling's engine does not: it reads any stored
  annotation of a kind it knows, "whatever the switches say", and under `trust`
  takes it "as given" (decision 135's addendum of 2026-10-23; ticket 14, comment 5;
  ticket 26 lists "whether the engine uses an annotation stored by another
  producer or recomputes it" as open). So a table from another producer that
  follows the current specification, or an `INT64 MILLIS` table, has its
  truncated upper bound read as a stored value, and the probe's MILLIS rows show
  comparisons turning empty. This undermines the claim #3 makes, and bears on
  section 16.
- **Contradiction: where Pathling's warning ends.** Ticket 14's second report
  says "It does not throw"; decision 138 as amended and the code say the sinks
  refuse. The ticket text is stale. `contracts/storage-layout.md` agrees with
  the code.
- **Unverified claim in #3.** #3 says Spark 3 and later write the proleptic
  calendar by default (`datetimeRebaseModeInWrite=CORRECTED`). The repository
  evidence covers Spark 4 only ("Spark 4's default rebase modes are CORRECTED",
  ticket 14). My recollection, unverified, is that Spark 3.x defaulted to
  `EXCEPTION`. Reword the claim to Spark 4, or check, before it stands.
- **Placement.** #3's text and example use the sibling names (`__birthDate_start`);
  Pathling stores `_birthDate.__start` (section 3).
- **Dependence on ticket 45.** After the specification's type is final, ticket 45
  either returns to `INT96` (removing `StoredTimestamps`, `TimestampStorage`, the
  warning and the sessions' setting, after checking `INT96` works through the
  footer, comparisons, a Delta merge and data written before and after) or keeps
  `TIMESTAMP_MICROS` and documents the session setup, Databricks first.
- **If rejected, with `int96` and MILLIS kept.** Pathling stays a deviation
  (decision 138, forever). Matching the specification's unit would mean moving the
  engine's computed bounds to milliseconds too, or the date fast path changes
  results (criterion 3 of ticket 14).
- **If `int96` is added as allowed.** Remove the sink check and the warning, and
  decide what a reader does with each form; no Pathling code reads the physical
  type today.

---

## 7. `instant` carries range annotations

**Kind:** amendment proposed. **Status:** raised upstream, item 3 of
[aehrc/parquet-on-fhir#3](https://github.com/aehrc/parquet-on-fhir/issues/3).

### Context

**What the specification says.** "Date ranges": "Any element with the `date` or
`dateTime` data type can be annotated". `instant` is a primitive type in the type
table (binary, `STRING`) and is not named.

**What Pathling built.** Decision 138 and `LayoutFields` annotate `date`,
`dateTime` and `instant`, so `meta.lastUpdated` is annotated in every resource
that has one (ticket 14, comment 2). An `instant` compares as a `dateTime` in the
engine. `time` is not annotated.

### Issue

`instant` is a `dateTime` stated to at least the second and with a timezone, so
its range is well defined and it compares with dates and dateTimes, and
`meta.lastUpdated` compared with a date is common. The specification neither
includes nor excludes it, so producers disagree.

### Proposal

As raised in #3: "Any element with the `date`, `dateTime` or `instant` data type".

### Other considerations

- **Cost.** Two 8-byte columns per resource for `meta.lastUpdated`, plus the group
  `_lastUpdated`. Not measured. `evidence/dense-annotation-width.md` measures the
  extra width of a dense schema and is flagged there as provisional.
- **Independence.** It does not depend on the range type or the placement and can
  be agreed alone.
- **If rejected.** Remove `instant` from the range-annotated types in
  `LayoutFields`; the engine computes the range from the text. The
  `contracts/storage-layout.md` row loses its "instant" sentence.

---

## 8. The canonical annotation's value type

**Kind:** amendment proposed. **Status:** raised upstream,
[aehrc/parquet-on-fhir#1](https://github.com/aehrc/parquet-on-fhir/issues/1).

### Context

**What the specification says.** "Quantity unit canonicalization": a Quantity can
carry a `canonical` annotation, "a group" with `value`
(`fixed_len_byte_array(16)`, `DECIMAL(precision=38, scale=6)`) and `code`
(`binary`, `STRING`), for example to canonicalise temperatures to Kelvin. It does
not say which units are canonicalised or how.

**What Pathling built.** Pathling first emitted both the specification's
`__canonical` and its own `__canonical_exact` (decision 46). Decision 137
(owner, 2026-10-26) reversed that: a quantity carries only `__canonical_exact`,
because nothing read `__canonical` and the fixed scale makes quantities that
differ by orders of magnitude compare equal. `__canonical_exact` is
`struct<value struct<value decimal(38,0), scale int>, code string>` (ticket 12):
the canonical value as a scaled integer with the decimal point anywhere, and the
canonical unit code beside it, because a value without its base unit lets one
metre and one second compare equal.

### Issue

As raised in #1: unit conversion multiplies by a power of ten set by the unit, but
the scale is fixed at 6, so `1 ng`, `250 pg` and `4 ng` (as grams) all become
`0.000000` and compare equal, silently, and below about 10^-4 a canonical value has
fewer than three significant figures. Unlike the other annotations, the canonical
value has no lossless counterpart beside it, so a consumer that wants correctness
must recompute and the annotation loses its purpose.

### Proposal

As raised in #1, four directions in order of preference: (1) mirror the pattern
for elements, a `STRING` lexical form with the `DECIMAL(38,6)` beside it; (2) a
scaled-integer group, `{ value: DECIMAL(38,0), scale: INT32 }`; (3) an
implementation-chosen numeric type declared in the schema; (4) keep
`DECIMAL(38,6)` and document the unit families it cannot serve.

**Two requirements on whichever type is chosen** (added 2026-11-05).

1. **One stored representation per magnitude (proposed as a requirement).** Two
   equal quantities SHALL have byte-equal canonical values, so that SQL equality,
   hashing, `GROUP BY` and joins on the stored annotation give the right answer.
   - Pathling's stored form breaks this for offset units
     [M, `evidence/annotations-layout-assessment.md`, section 5]. `37 Cel`
     and `98.6 [degF]` store as (31015000000000000000000, scale 20), while
     `310.15 K` stores as (31015, scale 2). SQL then sees two values for one
     temperature.
   - Within a unit, and across multiplicative conversions such as `mg` and `g`,
     it is already unique [M].
   - Pathling's way to meet it is to strip trailing zeros on the canonical path
     only, so the element's own value keeps its FHIR precision. This is decided
     and not yet built. A probe shows it makes all 13 groups tried unique [M]. It is an
     implementation, not specification text: the specification states the
     property, and each producer chooses how to meet it.
2. **An ordered type with statistics, if the annotation is to let readers skip
   row groups.** This is stated as a consequence, for the specification owner
   to weigh, not as a proposal. Pathling's engine design adds a plain condition
   on the stored annotation beside each comparison (decision D11, 2026-11-03).
   That condition helps only when the type's Parquet order is numeric order and
   the column has statistics, which rules out `INT96`-style types. Measured
   [M, `evidence/annotations-design-quantity-root.md`, part A]:
   - On the scaled-integer form, a plain predicate on the leaves is unsound: it
     lost 499 of 4,999 matches.
   - A sound superset on the same leaves skips nothing, because unscaled values
     are not ordered across scales.
   - Only a fixed-scale value skipped: 5,039 rows scanned for 4,999 matches.

**How the directions meet the two requirements.** None of these is a
recommendation.

| Direction | One representation per magnitude | Ordered, with statistics |
|---|---|---|
| (1) `STRING` lexical form + `DECIMAL(38,6)` | Text: only with a normalisation rule [I] | Text: no, string order is not numeric order [I]. The fixed-scale field: yes [M] |
| (2) Scaled integer `{value, scale}` (Pathling's `__canonical_exact`) | Only if trailing zeros are stripped [M] | No [M] |
| (3) Implementation-chosen | Depends on the producer | Depends on the producer |
| (4) `DECIMAL(38,6)` only | Yes [I] | Yes [M], but loses small magnitudes (#1) |
| (5, new) An order-preserving byte encoding | Yes, by construction [I] | Yes, in principle [I] |

Direction 5 is published practice: HBase `OrderedBytes` numeric encoding and
SQLite4's key encoding. It stores a sign byte, a biased exponent and a base-100
mantissa, and its byte order is numeric order [I]. Unverified:

- whether Spark pushes `<` and `>` on a binary column;
- whether Delta skips files on binary statistics. It is believed not to.

Its costs are an opaque value that every consumer needs a codec to read, and a
decode for arithmetic [I]. Keeping a fixed-scale value beside an exact one, as
the design doc proposes as optional, is the measured way to meet both
requirements together. The cost is a second field.

### Other considerations

- **Which direction Pathling wants.** Pathling's `__canonical_exact` is
  direction 2 plus a `code` field, but #1 ranks direction 1 first. A `STRING` form
  would cost the engine a parse per comparison or its own extra field. Pathling's
  plan, in #1, was to omit `canonical` and emit an equivalent under a different
  name, which decision 137 did. The owner should say which direction Pathling
  accepts.
- **The type is proposed twice.** Ticket 35 item 4 also offers `__canonical_exact`
  in place of the specification's. Settle the type here and the placement in
  section 3.
- **Evidence and cost.** `evidence/ticket-12-quantity-annotations.md` and ticket
  12's comments: with annotations a comparison `q > 100 'mg/dL'` over 10,000
  Observations takes 51 ms against 61 ms computed, and an annotated comparison
  makes no canonicalisation call; carrying a second canonical field cost a field in
  every stored quantity, a second canonicalisation per row where an unannotated
  quantity is returned whole, and a second UDF call per quantity at ingest
  (decision 137). The gain from the annotation is small on these figures.
  `evidence/t100o-quantity-decode-cost.md` has the decode measurements.
- **Precision limit remains.** The exact form is normalised to at most 38
  digits (ticket 12), so a value needing more is rounded. #1's direction 2 has
  the same limit.
- **Compatibility.** No known producer writes `__canonical`; the upstream
  example files carry none. Nobody reads Pathling's `__canonical_exact`.
- **Dependence.** Overlaps section 3 (placement) and section 9 (the rules the
  type depends on).
- **If rejected.** If the specification keeps `DECIMAL(38,6)`, Pathling keeps
  emitting `__canonical_exact` as a non-standard annotation (decision 137 already
  says it is replaced "if the specification adopts a magnitude-preserving
  representation"). Nothing is lost; interoperability on canonical quantities is.

---

## 9. Pathling emits `__canonical_exact`, and the canonicalisation rules the specification leaves open

**Kind:** deviation in Pathling only. **Status:** not yet drafted. The
substitution is recorded in decision 137, FR-005 and
`contracts/storage-layout.md`; the open rules below have no upstream text.

### Context

**What the specification says.** Only that a Quantity "can be annotated with the
`canonical` annotation to add canonical unit and value information". It says
nothing about which unit systems are canonicalised, what code the annotation
carries, or which table of units it follows.

**What Pathling built.** The kind canonicalises only a quantity whose system is
UCUM, or a definite calendar duration, which it maps to its UCUM equivalent in
Java (ticket 12, item 3, in its own commit `b54ccacafd`; owner confirmed
2026-10-26). A quantity with another system or none has `null` and compares only
with the same system and code. This follows FHIR's mapping to System.Quantity
(FHIRPath binding 2.9.1.3) and, for search, decision 77. The code carried is the
canonical UCUM unit. Pathling's `fhir-schema` takes UCUM from `ucumate-core`.

### Issue

Two producers using different rules write different canonical values for the same
quantity, and a consumer cannot tell which rule applied. Examples: a quantity
with a non-UCUM system, or a calendar duration (`1 'a'` against `1 year`). The
canonical value also depends on the UCUM table's version, and the annotation does
not record it.

### Proposal

Not drafted. A clarification would say: canonicalisation applies only to UCUM
(and the FHIR calendar-duration mapping); a quantity outside it has no canonical
annotation; the `code` is a UCUM code; and the annotation reflects the producer's
UCUM table, with a note on how a consumer treats a mismatch.

### Other considerations

- **Open question for the owner.** Whether to raise this with #1, or separately.
  Ticket 26 holds a related follow-up, annotation kinds as plugins, which covers
  "the identity of a kind", including "versioned computations, such as
  `__canonical_exact`'s dependence on the UCUM table, and data that outlives the
  engine that wrote it". It is the same problem, at Pathling's level.
- **Contradiction between decisions on UCUM.** Decision 77 had search follow FHIR
  search on UCUM while FHIRPath comparison "still canonicalises from the code
  whatever the system"; ticket 12's T096b then made comparison UCUM-only. The
  spec files record this as settled, so the rule above is Pathling's current one.
- **If the rules are not specified.** Pathling documents them in
  `contracts/storage-layout.md`, as it does now. Another producer's canonical
  annotation, if read, would be read as given under `trust` (section 16).

---

## 10. What the `numeric` annotation holds beyond six fractional digits and out of range

**Kind:** clarification. **Status:** not yet drafted.

### Context

**What the specification says.** "Decimal values": "Elements with a decimal type
can use the `numeric` to store the value in a numeric form", with
`fixed_len_byte_array(16)` and `DECIMAL(precision=38, scale=6)`. It says nothing
about a decimal with more than six fractional digits, or more than 32 integer
digits.

**What Pathling built.** The annotation is `_x.__numeric` at `DECIMAL(38,6)`, the
specification's type and the engine's System.Decimal (ticket 41, FR-035). It holds
"the text rounded to six fractional digits, or null where the value has more than
32 integer digits or the text is not a decimal" (owner, 2026-10-26;
`contracts/storage-layout.md`, "The decimal annotation"). It is the engine's own
decoding of the text, so the annotation and the lexical path cannot disagree.
Rounding follows Spark's safe cast, which I believe rounds half up; I did not
verify that. Placement is section 3.

### Issue

A consumer reading the annotation cannot tell a decimal that was rounded from one
that was exact, or a null that means "no value" from one that means "did not fit".
That is harmless only because the text is stored beside it. The engine's
System.Decimal is the same type, so it reads the annotation as the value, and a
value rounded at ingest is the value used in a comparison.

### Proposal

Not drafted. A clarification would say: `numeric` is the value rounded to six
fractional digits (the mode named), null where it does not fit in 38 digits or the
text is not a number, and the text remains the authority.

### Other considerations

- **Not in scope here:** FHIRPath requires a Decimal to provide at least a step of
  10^-8, so a six-digit annotation is the reason Pathling's System.Decimal is
  `DECIMAL(38,6)` against FHIRPath's eight digits (ticket 41, ticket 26). That is
  a FHIRPath deviation, not a Parquet on FHIR one. It becomes an upstream matter
  only if Pathling moves to `DECIMAL(38,8)` (ticket 26 expects "a proposal
  upstream for the annotation's type"), because the specification fixes the
  annotation's scale at 6.
- **Compatibility.** The upstream example files carry `numeric` as
  `fixed_len_byte_array(16) (Decimal(precision=38, scale=6))` siblings, so the type
  matches what is published.
- **If not clarified.** Nothing changes in Pathling.

---

## 11. A decimal's text is numerically equal to the source, not its lexical form

**Kind:** deviation in Pathling only. **Status:** not yet drafted. No upstream
change is needed; it is tracked as OQ-1 and ticket 16, which decision 134 deferred
and which is not part of #2367.

### Context

**What the specification says.** "Decimal values": "The original values of
decimals are stored as strings to preserve the precision and scale of the original
data." The introduction promises a lossless representation, and "Primitive
elements" requires the data to "comply with the rules defined in the Data Types
section".

**What Pathling built.** Decimals are text with a numeric annotation (FR-002), but
the text is "numerically equal to the source, as a double holds it until OQ-1 is
answered". JSON ingest reads a number as a double and stores its text, so `1.50`
stores as `1.5`, and a number with forty significant digits is stored at a double's
precision; egress casts the text back to a double (decision 68;
`data-model.md`, "Type mapping"; script `evidence/scripts/double_serde.py`). XML
ingest keeps the lexical form (ticket 29's comment).

### Issue

The deviation defeats the stated reason for storing a decimal as text, and the
claim of losslessness. Pathling's own contract records it, but it is not in the
deviation table of `contracts/storage-layout.md` as a deviation from the
specification: the table row says only that the text "is numerically equal to the
source, not in the source's lexical form".

### Proposal

None to the specification. Pathling intends to keep the lexical form once
ticket 16 reworks ingest, restoring the specification's guarantee.

### Other considerations

- **Effect on other changes.** It is one of the exceptions to the round-trip
  invariant of section 4. It is why ticket 29's version bump for ticket 16 is
  expected (section 1).
- **Scale of the data at risk.** Data written before ticket 16 is the JSON data
  with decimals passed through a double; XML data is already lossless
  (ticket 29's comment, owner, 2026-10-22).
- **If the lexical form stays unrepresented.** The invariant in section 4 cannot be
  claimed for Pathling.

---

## 12. Primitive ids and extensions: repeating primitives, positional nulls and `_x` without `x`

**Kind:** amendment proposed. **Status:** not yet drafted. Ticket 17 is a
placeholder, "to be rewritten once ticket 32 lands".

### Context

**What the specification says.** "Primitive extensions and internal identifiers":
the id and extensions of a primitive "can be represented ... as a field ... with
the same name prepended with an underscore", "represented as a group within the
schema" with fields `id` (`binary`, `STRING`) and `extension` ("As per the
Extension data type"). The example shows a singular primitive only. Two things in
it are not consistent with the rest:

- the example schema gives `_birthDate.extension` as a plain group with `url` and
  `valueDateTime`, although the "Extensions" section represents extensions as a
  `LIST` (`optional group extension (LIST) { repeated group list { optional group
  element {...} } }`) and an element can have several;
- nothing says what `_x` is when `x` repeats. "Repeating elements" makes `x` a
  `LIST` of values, and `_x` as "a group" with one `id` and one `extension` cannot
  say which of the values the id belongs to.

**What Pathling built, and has not yet.** The design follows FHIR's own JSON
(`"given": ["Jane", null]` beside `"_given": [null, {...}]`): `_x` is aligned with
`x` by position, singular and repeating alike, and a repeating primitive's `_x` is
an array of groups with a null in `x` holding the place of a value that exists only
in `_x` (decision 63, which says dropping that null shifts every position after
it). A primitive carried only as `_x` is stored as a typed null of its declared
type (decision 72). Where the schema carries `_x` but not `x`, `x` is null in every
row (decision 135's addendum). **Pathling does not yet store ids and extensions**:
FR-016 excepts them "until ticket 17 stores the metadata group", and the group
today holds annotations only. The reader emits the group only where something
populates it (decision 60's addendum).

### Issue

The specification, read literally, cannot hold a repeating primitive's metadata,
and does not say what a positional null in a value array means. Level 2 (section
4) excludes "a positional null with no aligned `_x`", which presupposes the rule.
The specification also lists only `id` and `extension` for `_x`, so a primitive's
group has no place to hold what section 3 puts there.

### Proposal

Not drafted. A proposal would: say `_x` has the same cardinality as `x`, so a
repeating primitive's `_x` is a `LIST` of groups aligned with the values by
position; allow a null in a value `LIST` only where the aligned `_x` item carries
content; allow `_x` without `x` and say the value is null; and correct the
`_birthDate.extension` example to a `LIST`. It should also say what a resource
without a domain-resource root does (ticket 17: `Binary`, `Bundle` and
`Parameters` have no root `extension`; the Extension shape is the same).

### Other considerations

- **Open question, from ticket 17.** "Decide whether `_id` and `_url` groups exist
  at all": `id` and an extension's `url` are themselves primitives, so by the
  specification's rule "each primitive element" can have an `_x` group, which would
  recurse. Ticket 17 has not decided.
- **Compatibility.** The upstream example files contain no `_x` group at all
  (checked by reading their schemas). The released encoder (before Pathling's PoF
  work) kept a metadata-only structure where the primitive is singular and dropped
  it where it repeats, because HAPI's parser discards an `_x` array with no `x`
  array. That is evidence the repeating case is easy to get wrong
  (decisions 72 and 71's addendum).
- **Evidence.** Decision 63: of the 2,911 FHIR example resources, 81 carry
  primitive metadata and none carries a null in a value array; the one null is in
  an `_given`.
- **Dependence.** Section 3 (annotations share `_x`), section 4 (positional
  nulls are the one null allowed in an array).
- **If rejected.** Pathling stores the group its own way and records the
  repeating-primitive rule as a deviation. A reader of the current specification
  would not find ids and extensions of a repeating primitive.

---

## 13. Elements of the abstract `Resource` type, `contained` first, are not represented

**Kind:** deviation in Pathling only. **Status:** not yet drafted.

### Context

**What the specification says.** "Resource type": a table's schema "SHALL be
derived from a single base FHIR resource definition". It says nothing about
`contained`, `Bundle.entry.resource`, `Parameters.parameter.resource`, or any
element whose type is the abstract `Resource`, and the introduction claims a
lossless representation of FHIR.

**What Pathling built.** `contained` is not represented and is dropped silently
(FR-006); an inline resource held by an element declared as the abstract
`Resource`, such as `Parameters.parameter.resource`, is dropped for want of a type
(FR-016 exception 2, decision 64). `Bundle` is never stored as a resource type: it
is accepted as a transport carrier and exploded into per-type tables (FR-007).
`contracts/storage-layout.md` lists `contained` and `Bundle` as deviations.

### Issue

A single base resource definition does not describe what an inline resource holds,
so no schema can be derived for it, and a conversion to FHIR cannot restore it.
The `Resource`-typed elements are not in the deviation list as a class, only
`contained`.

### Proposal

Not drafted. The clarification would say that elements of type `Resource` are
outside the layout, or define their representation (for example a column per
resource type, or a JSON string).

### Other considerations

- **Effect.** One of the exceptions to the invariant in section 4. The
  `contained` case is documented in the round-trip section of
  `contracts/storage-layout.md`.
- **Open question for the specification owner.** Whether the specification should
  define a representation, or declare these elements out of scope as Pathling
  does. Pathling has not decided it wants one.
- **If it is out of scope upstream.** No change. If the specification defines a
  representation, Pathling's FR-006 and FR-016 would change with it.

---

## 14. `unsignedInt` and `positiveInt` are stored as signed `INT32`

**Kind:** deviation in Pathling only. **Status:** not yet drafted. **It is not
recorded** as a deviation in `contracts/storage-layout.md`, FR-001's list or
`decisions.md`, as far as I found. (Decision 68 records the same kind of gap for
`base64Binary` and fixes it.)

### Context

**What the specification says.** The "Primitive elements" table: `integer` is
`INT32` with `INT(32, true)`, and `positiveInt` and `unsignedInt` are `INT32` with
`INT(32, false)`.

**What Pathling built.** The type mapping in `data-model.md` stores `integer`,
`unsignedInt` and `positiveInt` as "Integer", Spark's `IntegerType`, and ingest
accepts a long and stores an integer (decision 68, `evidence/decisions.md` line 1422:
"`unsignedInt` and `positiveInt` accept `LongType` and store `IntegerType`").
Spark has no unsigned 32-bit type, so it cannot write `INT(32, false)`.

### Issue

A conformant table that uses `INT(32, false)` is read by Spark as `LongType`, and
Pathling's behaviour on it is not documented. I did not test it. Conversely a
table Pathling writes does not carry the specified logical type. FHIR R4's range
for both types is 0 or 1 to 2,147,483,647, which fits a signed `INT32` (from my
recollection of the FHIR R4 data types page; verify).

### Proposal

Not drafted, and the right proposal is the owner's call. Options: allow signed
`INT32` for both types because the FHIR range fits; keep the unsigned type and
record the deviation; or say a reader SHALL accept either.

### Other considerations

- **Unverified.** I did not run a Spark write to see the footer, because a probe
  was refused in this session. The claim rests on `data-model.md` and the decision
  cited.
- **If the specification keeps the unsigned type.** Pathling deviates for these
  two types and should add them to its deviation table. It would also need to
  decide what its engine does with a stored `LongType` where it expects
  `IntegerType`.

---

## 15. Fields the definitions do not describe, and their place in the conformance levels

**Kind:** clarification. **Status:** not yet drafted. Ticket 36 scopes a Pathling
follow-up issue, which ticket 26 raises; it is not an upstream proposal yet.

### Context

**What the specification says.** "Field optionality": other fields "can be
optionally included as needed"; "Annotations": non-standard annotations are
allowed if they do not collide. It says nothing about a column the definitions do
not describe (another FHIR version's field, a vendor key, `fhir_comments`, a legacy
`_fid` or `_extension` column, another producer's `contained`), and its schema is
"derived from a resource definition".

**What Pathling built.** In `trust` (FR-063) ingest and the engine assume
conformance and check nothing; what they do with undescribed content is undefined.
`__` fields are layout, allowed at level 3 (decision 135); other undescribed
content is outside the round-trip invariant (ticket 35, ticket 34, now ticket 36).
Ticket 36 lists the forms an extra field takes and the questions: its level,
what `trust` promises, how ingest, the engine and egress treat it (user-supplied
definitions, inferred types, a JSON-type marker in the Parquet metadata, a
namespaced attribute in XML), and whether the invariant can extend to it.

### Issue

The specification's schema is derived from the definitions, but a table may carry
more. Without a rule, conformance depends on the implementation, and a producer
cannot know whether a column it adds makes its table non-conformant.

### Proposal

Not drafted. It would say how an extra column interacts with conformance, once
Pathling has decided what it does with one.

### Other considerations

- **Not decided.** Ticket 36's status is ready-for-human; #2367 delivers none of it
  (owner, 2026-10-22). The questions of section 2 (ignoring versus preserving) and
  section 4 (levels) are the upstream-visible part.
- **If not raised.** No Pathling change; the follow-up is Pathling's own.

---

## 16. What an annotation means when it is null, when tables are merged, and who may trust it

**Kind:** clarification. **Status:** not yet drafted (tickets 38 and 26; decision
135's addenda).

### Context

**What the specification says.** The introduction: "multiple Parquet tables could
exist for the same resource that have different fields in their schemas, and
... these schemas could be merged to produce a single table with the union of those
fields." "Annotations": "derived forms of element values that may be useful for
querying". It does not say what a null annotation beside a present value means, or
whether a consumer may rely on a stored annotation.

**What Pathling built.** The engine decides whether to use an annotation from the
schema, never per row (FR-023), and under `trust` takes a stored annotation as
given. It computes the same value on demand where the schema lacks it (FR-022).
The annotation step's default, *fill*, keeps a stored annotation, including a null
in a row, where the schema carries the field.

### Issue

Merging tables is something the specification invites. Ticket 38's example:
January's Patients are encoded with the range kind on and February's without it.
Read together, the schema carries `_birthDate.__start`, and February's rows hold a
null. "This matters beyond repair. *Fill*, the default, can make the engine silently
wrong on merged data: the engine reads a stored annotation wherever the schema
carries it, so a null annotation beside a non-null value takes the null fast path
and answers as if the value were absent." The specification gives no way to say
that a null annotation means "not computed". The same applies to a producer whose
annotation is wrong, such as the millisecond ranges of section 6.

### Proposal

Drafted 2026-11-05 for the first point. The rest is not drafted.

**Proposed text (a SHOULD).** Add to "Annotations": "Where a table's schema
carries an annotation that is derived from its element's value (the date range,
the decimal `numeric`, the quantity `canonical`), a producer SHOULD write a
non-null annotation in every row where the element's value is non-null, and a
null annotation where the value is null."

- **Why a SHOULD and not a MUST.** Pathling's engine does not depend on the rule.
  A per-row fallback, using the annotation where it is present and computing from
  the value where it is not, remains possible. That design question is deferred
  in Pathling, and its intended answer is per row (owner, 2026-11-04).
- **What it prevents.** Take a stored `birthDate` of `1980` with a null
  `__start`, and the filter `where(birthDate >= @1975)`:
  - an engine that trusts the schema and reads the annotation returns empty;
  - an engine that computes from the value returns true [I].

  Merged tables (ticket 38's example) produce exactly this.
- **Not covered.** Accelerator annotations, such as a hash proposed later, MAY be
  null and never decide a result alone. An annotation on a null value is
  ignored.

Not drafted: whether a consumer MAY treat an annotation as authoritative where it
disagrees with the element, and how a merge treats annotations.

### Other considerations

- **Risk that remains under a SHOULD** (added 2026-11-05). A producer that
  backfills only part of a table makes an engine that trusts the schema drop rows
  silently. Pathling's writer meets the rule by construction [I]. Pathling may
  offer a diagnostic outside the query, which only logs.

- **Open question for the owner:** Pathling's answer is per schema and trusts
  (decisions 135, 31), and the replan proposal moves ticket 38 after #2367. The
  specification owner may prefer the opposite rule. Pathling has not decided
  whether the engine should recompute an annotation another producer wrote.
- **Pathling-side follow-up.** Ticket 26 lists "whether the engine uses an
  annotation stored by another producer or recomputes it" and "a stored annotation
  that disagrees with its value".
- **If not clarified.** Pathling's docs say a merged table needs *recompute*
  (replan proposal, post-2367 list).

---

## 17. The range of a value that states a time but no timezone

**Kind:** clarification. **Status:** not yet drafted (ticket 26).

### Context

**What the specification says.** "Date ranges": `start` is "the earliest instant
that is considered to be included in the value" and `end` "the latest".

**What Pathling built.** A `dateTime` or `instant` that states a time but no
timezone is valid for ingest and the engine (decision 133's addendum, owner,
2026-10-21), although FHIR requires a timezone with a time. Its range is the range
the engine computes for it, taking the value as UTC (ticket 14; ticket 26 records
the question). `contracts/storage-layout.md`: "A time without a timezone is taken
as UTC, as the engine takes it".

### Issue

By the specification's own wording the earliest instant included in a value whose
zone is unknown is up to 14 hours earlier and the latest up to 14 hours later, so
a UTC assumption stores a range narrower than the definition allows. Such values
are common in EHR extracts (`evidence/ingress-strictness/README.md`).

### Proposal

Not drafted. A clarification would say how a producer writes the range of a value
without a timezone, or that the annotation is null for it.

### Other considerations

- **Pathling-side.** Whether to widen by 14 hours, in the engine and the annotation,
  is a follow-up (ticket 26). The answer should be the same in both.
- **If not clarified.** Pathling keeps UTC and documents it.

---

## Not in scope

- **FHIRPath's eight fractional digits for Decimal** against System.Decimal's six
  (ticket 41, FR-035) is a FHIRPath deviation. It concerns this document only in
  that the specification fixes the `numeric` annotation's scale at six (section
  10).
- Dense schemas, the XML sink, the shape of SQL output, the server and the legacy
  layout are Pathling matters with no change asked of the specification.

## Cross-cutting findings for the Pathling owner (remove before sharing)

Contradictions and gaps found between tickets, decisions, code and the
upstream specification, in the order the sections give them:

1. **Untracked upstream issues.** #1 and #2 are not recorded in any ticket.
   Ticket 30's boxes are unticked though #2 exists. The re-plan's list of
   amendments (30, 35, 44 and #3) omits #1.
2. **Unmarked data.** #2 requires readers to refuse unmarked tables; FR-039,
   ADR-0006 and ticket 29 accept them.
3. **Version rule.** #2 raises the major version only for misreads; ticket 29
   expects a bump for ticket 16's decimals.
4. **Name.** `__layout_v1` in FR-065, ADR-0006 and `CONTEXT.md` against `__pof_v1`
   in #2.
5. **Unmeasured claims in #2.** `evidence/layout-column.md` does not exist; only the
   null-type refusal is measured.
6. **#3 and placement.** #3's text and examples use sibling names; Pathling stores
   annotations in the container (decision 135). *Revised 2026-11-05:* section 3 no longer
   proposes the container; it sets out four placements without a preference, so
   #3 follows whichever is chosen.
7. **#3 and the reader.** #3 says a reader computes the range when it does not
   find the expected type; Pathling's engine reads any stored annotation under
   `trust`, so foreign millisecond ranges give wrong answers.
8. **#3 and Spark 3.** The claim that Spark 3 and later write the proleptic
   calendar by default is unsupported by the repository, which covers Spark 4
   only.
9. **Decision 138 against ticket 44.** *Revised 2026-11-05.* INT96's lack of
   statistics is now measured (`evidence/annotations-design-pushdown.md`,
   result 5), and the shape that would use the pushdown is known and adopted in
   the engine design (D11), though not built. MICROS against NANOS was
   unrecorded; section 6 now gives the range ground (int64 NANOS covers only
   1677–2262). A line in decision 138 should record it.
10. **Ticket 14's text** says the context does not throw; the code and decision
    138 say the sinks refuse.
11. **Canonical type proposed twice** (#1 and ticket 35 item 4), with #1's
    preferred direction a `STRING`.
12. **Ticket 35 item 3 and the code.** `resourceType` is already written in every
    row.
13. **Ticket 35's invariant against FR-016.** The invariant it proposes has
    exceptions Pathling itself has (decimal form, `contained`, base64 whitespace,
    primitive metadata until ticket 17).
14. **Unrecorded deviations.** `unsignedInt` and `positiveInt` stored as signed
    `INT32`, and the `required` marking of `resourceType`. Neither is in
    `contracts/storage-layout.md`.
15. **Specification defects found.** The `_birthDate.extension` example is a
    plain group where the text says `LIST`; the Quantity example copies the
    whole Quantity shape where the text lists `value` and `code`; the example
    Parquet files declare `resourceType` `optional` and many groups `required`,
    against "Field optionality"; the README's range annotation example cannot be
    written (#3).
16. **Overstated conformance wording.** `contracts/storage-layout.md` says its
    deviations are "all permitted by the specification". That does not hold for
    several of them: `__start` and `__end` have no standard name under the
    specification's sibling rule, the metadata group carries fields the
    specification does not list for `_x`, and sections 13 and 14 are not
    permitted by any specification text. It also lists primitive ids and
    extensions as "the specification's `_field` groups" although Pathling does
    not store them yet (ticket 17). Treat "permitted" as an open question.

Not accessed or not verified:

- The GitHub MCP server does not connect here. I used `gh`, which worked for all
  reads. There are no upstream pull requests and no comments on #1 to #3.
- A Spark write probe to check what Pathling's and Spark's own Parquet footers
  declare for `resourceType` (`required` or `optional`), for `positiveInt`
  (`INT32` logical type) and for the default calendar rebase modes was refused,
  so sections 5, 6 (Spark 3 default) and 14 rest on the repository's documents
  and, for the upstream example files, on reading their schemas with pyarrow.
- I recalled, without checking, that INT96 has been written as local time by older
  Hive and Impala, that FHIR R4 gives `unsignedInt` and `positiveInt` a signed
  32-bit range, and that Spark's cast rounds half up. All three are marked where
  they appear.
