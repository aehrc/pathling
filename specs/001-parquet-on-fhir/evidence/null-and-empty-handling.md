# Nulls and empty objects — open investigation

**Status: option A taken, as decision 71's addendum.** Recorded 2026-09-23 on
`issue/2761` (PR #2767), after round 4 of the M1 adversarial review. The
specification now carries the restated assumption and marks FR-019's deferral
wherever it was still promised, and T080c (`DeferredPruningTest`) pins the cases
§5, §11 and §12 describe. Round 5 found §12, and decision 72 fixed it. The file
is kept as the evidence behind that decision. The follow-up section measures
what the engine sees, and ends in a guiding principle, the options and the
recommendation.

Scripts: `scripts/NullProbe.java`, `scripts/OnlyNull.java`,
`scripts/SparkJson.java`, `scripts/LayoutProbe.java`, `scripts/SlotProbe.java`,
`scripts/MetadataSlotProbe.java`, `scripts/BackboneProbe.java`,
`scripts/GapProbe.java`.
How to run them is at the bottom.

## The question

Decision 71 deferred empty pruning out of M1 on a stated assumption: *the input
JSON and input PoF do not include elements that require pruning*. Round 4 of the
review found the assumption is violated by conformant R4 input, and the finding
holds up — I reproduced both counterexamples.

Neither input contains anything empty. The transform **creates** the emptiness,
because the layout is projected onto the FHIR definitions and whatever they do
not describe is dropped:

| Input | Returned |
|---|---|
| `{"name":[{"family":"Keep"},{"_family":{"extension":[…]}}]}` | `{"name":[{"family":"Keep"},{}]}` |
| `{"maritalStatus":{"text":["a","b"]}}` (fails the column type check) | `{"maritalStatus":{}}` |

The first is conformant R4 — the data-absent-reason pattern. The loss is
reported as a finding and a WARN, so FR-017 holds; what does not hold is the
claim that the assumption is about the input rather than about the corpus.

So: **is the deferral sound, and if not, what replaces it?** Three candidates,
and the evidence below bears on which:

1. Document the deferral honestly and leave the behaviour (round 4's ask).
2. Prune at ingest, so the layout itself never carries an empty structure.
3. Wait for the custom JSON serde that the decimal invariant also waits on
   (T134h, tied to T132/T135) and do both there.

## What was measured

### 1. The current encoders already store nulls in primitive arrays

`scripts/NullProbe.java`. HAPI keeps the positional placeholder when parsing
`"given":["Ann",null,"Bee"]` — a three-element list whose middle `StringType`
has a null value — with or without an accompanying `_given`. `SerializerBuilder`
maps over that list with `MapObjects` and compacts nothing, so:

```
ptc.encode(json, "Patient").selectExpr("name[0].given")  →  [Ann, null, Bee]
```

This is **pre-existing, live behaviour of the released encoders**, not something
the io layer introduces. Preserving positional nulls at egress (the second
addendum to decision 63) makes the new layout agree with the old one; the
pruning that decision 71 removed is what diverged.

### 2. The evaluator tolerates them, with two conventions

Same script, through a `ViewDefinition` over the encoded dataset. Nothing threw,
across everything tried:

| Expression | Result |
|---|---|
| `name.given` | `[Ann, null, Bee]` |
| `name.given.count()` | `3` |
| `name.given.exists()` / `.empty()` | `true` / `false` |
| `name.given.first()` | `Ann` |
| `name.given[1]` | empty |
| `name.given.where($this = 'Ann')` | `[Ann]` |
| `name.given.join(',')` | `Ann,Bee` |
| `name.given.distinct()` | `[Ann, null, Bee]` |
| `name.given.select($this + '!')` | `[Ann!, null, Bee!]` |

Element-wise operations follow FHIRPath's null-is-empty rule and skip the hole.
The cardinality operations are built on `size()`
(`ColumnRepresentation.count():454`, used by `ExistenceFunctions.count():101`)
and count it. `scripts/OnlyNull.java` sharpens this on a name whose only `given`
is value-less:

```
stored = [null]
name.given.exists()  → true
name.given.count()   → 1
name.given.empty()   → false
name.given.first()   → empty
```

`exists()` says there is a given name; asking for it returns nothing. With the
default encoder settings the primitive's extension is not retained either, so
the placeholder survives while the only thing it exists to align with does not.

**This is a pre-existing engine wart, not a consequence of anything in this
branch**, but it is the primitive-level twin of the M2 hazard recorded in T134h:
a present-but-empty container makes `exists()` true. Worth deciding whether that
belongs in this programme or its own issue.

### 3. The spike did nothing about any of it

Worktree `.claude/worktrees/spike/parquet-on-fhir`, detached at `8891777ca1`.

`io/src/main/java/au/csiro/pathling/schema/DatasetTransformer.java` is 189 lines
of pure per-type transform (decimal, base64, integer widths). No `isNotNull`, no
`size()`, no `filter`, nothing conditional. The writer is one line
(`FhirJsonWriter.java:61`): transform, then `.write().json(path)`. Everything
about nulls is Spark's own JSON reader and writer.

And that suffices, because Spark gets the alignment case right unaided
(`scripts/SparkJson.java`):

```
--- name[0].given:  ArraySeq(Ann, null, Bee)
--- name[0]._given: ArraySeq(null, [extension…], null)
--- written: {"id":"1","name":[{"_given":[null,{"extension":[…]},null],
                                "given":["Ann",null,"Bee"]},{"family":"Only"}],…}
```

Byte-for-byte alignment in both arrays. `ignoreNullFields` drops null *fields*;
null *elements* are written as `null`.

**The spike never faced our problem because of its schema, not its code.** It
read the inferred schema wholesale and kept every column in it — including
`_given`, and including undescribed content:

```
--- written: {"id":"2","name":[{"bogusChild":"y"},{"family":"Smith"}],…}
```

Nothing is ever discarded, so no element is ever left empty. The pruning problem
is **created by the definition-driven projection** (FR-011, FR-044), which the
spike did not have. It bought immunity by letting the data define the layout,
which is exactly the property this programme gave up on purpose.

Its corpus also never contained the case: zero `null` literals and zero `_x`
fields across all six fixtures (`anne.Patient`, `bodyTemp.Observation`,
`withErrors.ExplanationOfBenefit`, `cpt.ValueSet`, `gcs.QuestionnaireResponse`,
`photo.Binary`), compared with `JSONAssert` in `STRICT` mode. So it is silent on
the DAR case rather than reassuring about it.

### 4. Where the spike would have landed is where we are

Given a struct whose fields are all null, Spark's writer emits an empty object:

```
input:   {"name":[{"family":null},{"family":"S"}]}
written: {"id":"3","name":[{},{"family":"S"}],…}
```

`{}` — the same output decision 71 now accepts, from Spark's writer rather than
ours. This corroborates `spark-type-findings.md`'s JSON egress table.

## The ingest-pruning design, as far as it got

**Superseded.** As sketched, this design prunes a structure whose only content
was primitive metadata, which changes query answers (follow-up §7). It survives
as option B′ below, with `_x` counted as presence. Not implemented; no code written. Sketched against the current
`ResourceTransformer` and reviewed in conversation only.

**Rule.** A structure is stored only where some leaf beneath it stores
something. A complex array keeps only the items that store something, and is
null where none do.

**Mechanism.** A presence planner beside `field`/`value`/`structure`, returning
a predicate rather than a value:

- leaf → `storedValue(source, type, observed).isNotNull()` (`:322`) — a
  `try_cast` over a field access. For a column the converter refuses this is
  `lit(null).cast(...)`, so the predicate folds to false and the leaf leaves the
  plan statically.
- structure → `value.isNotNull()` and the OR over its children's presence.
- complex array → `exists(source, element -> presence(element))`.

**The discipline that matters**, and the one the deleted `EmptyPruning` broke:
presence is built from the **source leaves**, never from the built stored
columns. That is what keeps it O(leaves under the node) instead of doubling per
level. The old code reduced `Column::isNotNull` over the same `fields` array it
then passed to `functions.struct(fields)`, and referenced a filtered array twice;
depth 12 exhausted the heap in 29s. `NestingDepthRoundTripTest` now pins the
linear behaviour and must keep passing.

**Two call sites.** `structure()` (`:247`) becomes
`when(value.isNotNull().and(presence.apply(value)), struct(fields))`; the
repeating branch of `value()` (`:235`), for arrays of structures only, becomes
`when(exists(source, presence), filter(transform(source, stored::apply), isNotNull))`.

**Egress does not change**: it keeps the single
`when(stored.isNotNull(), struct(...))` per level and the bare `transform`
(`egressValue`, `:456`), because the invariant then holds in the data rather
than being asserted about it.

**Do not prune primitive value arrays.** `["Ann",null,"Bee"]` keeps its null:
FHIR requires the placeholder to stay index-aligned with `_given` (decision 63,
T061). Complex arrays have no such sibling, and `ele-1` says an empty item is
not an element, so dropping one there is safe. Measurement 1 above says the
carve-out also matches what the encoders have always done.

**What it buys.** The DAR case becomes `{"name":[{"family":"Keep"}]}` — the
array shortens instead of carrying `{}`, which is decision 66's behaviour
returning, already on FR-016's exception list. Nothing empty reaches disk, so
from M2 a query's `exists()` answers on the stored columns rather than on what
the writer happened to leave. It restores the spike's property under a
definition-driven schema.

**What it costs.** ~80 lines plus tests, and spec churn: an amendment or
successor to decision 71 saying the pruning moved to ingest rather than out of
the milestone; decision 66's second addendum reverts; the `{}` assertion in
`PrunedSchemaGuaranteeTest.writesAnEmptyObjectWhereIgnoringContentEmptiesAnArrayItem`
goes back to asserting the shorter array; T080 returns in a new place; T134h
narrows to the decimal serde plus egress-side detection.

**The residue.** Egress would still trust the layout. Parquet written in this
layout by another producer, carrying an all-null struct, still comes back as
`{}` — the assumption moves from "the input is clean" to "the layout is clean",
much smaller but not nothing. The serde is what closes it.

**Open question, unanswered.** Pruning at ingest means the stored data no longer
records that the source had a second `name` at all. The finding still reports it
at read time, but nothing downstream of storage can tell. That is already true
of every ignored-content case, so it extends an existing property rather than
introducing one — but it is a deliberate choice, not an obvious one.

## Follow-up: what the engine sees

Recorded 2026-09-23, same branch and commit. Three probes:

- `scripts/LayoutProbe.java` encodes four Patients through both the released
  encoder (`PathlingContext.encode`) and `ResourceTransformer.toLayout`, then
  evaluates the same expressions over each through a `ViewDefinition`. The
  documents are the fhirpath.js fixture `patient-example-2.json`, which is the
  only reference fixture with value-less primitives (`given: [null,"Peter","James"]`
  beside a four-item `_given`), and three synthetic ones: the DAR `family` case
  above, a `_given` longer than `given`, and `["Ann",null,"Bee"]`.
- `scripts/SlotProbe.java` takes the DAR document's new layout and builds two
  pruned variants by hand, to measure what pruning would do before building it.
- fhirpath.js 3.16.4, the version the reference suite is taken from, run over
  the same documents as the spec's ground truth.

The reference suite was also run with its #437 exclusions switched off.

### 5. Without pruning, the new layout agrees with the old one

| Document | Old stored `name` | New stored `name` |
|---|---|---|
| DAR `family` | `[{family:x,_fid},{_fid},{family:y,_fid}]` | `[{family:x},{},{family:y}]` |
| `patient-example-2` | `given:[null,Peter,James]` | `given:[null,Peter,James]` |
| `_given` longer than `given` | `given:[Ann]` | `given:[Ann]` |

The `{}` is not new. The released encoder has always stored the metadata-only
item as a present struct, which only its `_fid` keeps non-null. HAPI does not
pad `given` to the length of `_given`, so neither layout has a fourth given in
`patient-example-2`.

Every expression that could be evaluated gave the same answer on both layouts:

| Expression (DAR `family`) | Old | New |
|---|---|---|
| `name.family` | `[x, y]` | `[x, y]` |
| `name.family.count()` | `2` | `2` |
| `name.count()` | `3` | `3` |
| `name.where(family.exists()).count()` | `2` | `2` |
| `name.where(family.empty()).count()` | `1` | `1` |

| Expression (`["Ann",null,"Bee"]` and `patient-example-2`) | Old | New |
|---|---|---|
| `name.given` | `[Ann, null, Bee]` | `[Ann, null, Bee]` |
| `name.given.count()` | `3` | `3` |
| `name.given[3]` | empty | empty |
| `name.given.distinct().count()` | `3` | `3` |

Two conventions, then, and both layouts share them. A **singular** primitive
reached through a complex array drops the hole, so `name.family.count()` is 2.
A **repeating** primitive keeps it, so `name.given.count()` is 3. The
metadata-only **complex** item counts as an element.

The only divergence was `FIELD_NOT_FOUND` when an expression named a field that
the pruned new schema did not carry (`name.given` on a document with no given at
all). That is the absent-field tolerance owed by Phase 7 (T101–T113) and has
nothing to do with this question.

### 6. What the spec says the answers are

fhirpath.js on the DAR document:

| Expression | fhirpath.js | Both layouts |
|---|---|---|
| `name.count()` | 3 | 3 |
| `name.family.count()` | 3 | 2 |
| `name.where(family.empty()).count()` | 0 | 1 |

The second `name` exists, and so does its `family`, because it carries an
extension. Both layouts get `name.count()` right. They get the other two wrong
because neither stores the primitive metadata, which is #437. The target until
M5 is therefore `name.count()` = 3, with `family` following once `_family` is
stored.

### 7. Pruning a structure to null is the same as dropping it

`SlotProbe`, on the DAR document:

| Expression | Old | `{}` (as now) | `null` in slot | dropped | fhirpath.js |
|---|---|---|---|---|---|
| `name.count()` | 3 | 3 | **2** | 2 | 3 |
| `name.family.count()` | 2 | 2 | 2 | 2 | 3 |
| `name.where(family.empty()).count()` | 1 | 1 | **0** | 0 | 0 |
| `name.select(family).count()` | 3 | 3 | **2** | 2 | — |
| `name[2].family` | `y` | `y` | **empty** | empty | — |

The engine compacts a null out of an array of structures when it traverses one.
The null placeholders it keeps are those in primitive arrays (measurement 2),
which is where `size()` sees them. To a query, then, pruning an empty structure
to null and keeping its slot is exactly the same as shortening the array, down
to shifting the indexes. Spark's writer also renders the slot as `null`, which
FHIR JSON does not allow in a complex array. So for a repeating element there
is no middle setting between keeping the structure and dropping it.

The same holds for a singular structure. A `period` whose only content is
`_start` exists by the spec, and `exists()` answers true on the released
encoder. Nulling it makes that answer false.

So the ingest pruning sketched above, whether it drops the item or nulls it in
place, gives `name.count()` 2 and `name.where(family.empty()).count()` 0 on the
DAR document. That departs from the released encoder and from the spec, and it
stays that way until M5. The sketch calls this "decision 66's behaviour
returning", but decision 66 covers content the definitions do not describe or
contradict (FR-016 item 5). The DAR element is conformant, so the two are
different cases.

### 8. The #437 exclusions are about unstored metadata, not about pruning

`YamlReferenceImplTest` with `-Dau.csiro.pathling.test.yaml.disabledExclusions=#437`
produced 29 failures. Every one is metadata that is not stored: `.id` on a
primitive, `extension()` on a primitive, `given[3]` (which exists only in
`_given`), and `Patient.name.given.count() = 4` (3 on the released encoder,
because HAPI does not pad `given`). None involves a complex element that was
emptied. The FHIR cases load through the same path as `PathlingContext.encode`,
which is HAPI followed by `FhirEncoders` (`FhirResolverFactory`,
`HapiResolverFactory`). Only the `Functions.*` fixtures go through Spark JSON
inference, in `ArbitraryObjectResolverFactory`.

### 9. The conformance suites cannot tell the options apart

A scan of every JSON and YAML fixture under `fhirpath/src/test/resources` found
no object whose only content is `_x` metadata. The detector was checked against
the DAR shape first. The primitive-level cases (`given[3]`, `** null is not
empty`, `** exists for null should return true`) sit on value arrays, which no
option below prunes, and are already excluded under #437 or as wontfix. So no
option moves a fhirpath.js or PTL case, and the reference suites are no
regression control for this choice. Whatever is decided needs its own pinned
test.

### 10. Complex-element metadata is stored, so it empties nothing

`scripts/MetadataSlotProbe.java` and `scripts/BackboneProbe.java`, same commit.
The recommendation below rests on the metadata-only structure being the only
empty one that conformant input can produce. That holds only if an element's own
`id`, `extension` and `modifierExtension` are stored content. If any of them
were not, a structure holding nothing else would be a second conformant source
of `{}`. They are all stored:

| Case | Released encoder | New layout, stored and written | Count, both layouts |
|---|---|---|---|
| `name` item with only `id` | `{id, _fid}` | `{"id":"n2"}` | `name.count()` 3 |
| `name` item with only `extension` | `{_fid}`, extension in `_extension` | `{"extension":[…]}` | 3 |
| `contact` item with only `id` | `{id, _fid}` | `{"id":"c2"}` | `contact.count()` 2 |
| `contact` item with only `extension` | `{_fid}` | `{"extension":[…]}` | 2 |
| `contact` item with only `modifierExtension` | `{_fid}`, dropped | `{"modifierExtension":[…]}` | 2 |
| singular `hospitalization` with only `modifierExtension` | `{_fid}`, dropped | `{"modifierExtension":[…]}` | 1, `exists()` true |
| `name` item with `id` and the DAR `_family` | `{id, _fid}` | `{"id":"n2"}` | 3 |

Every item keeps its slot, and the JSON written is the JSON read. The last row
shows that a stored sibling, even an `id` alone, keeps a structure whose
primitive metadata was dropped from being empty. So `{}` needs a structure whose
**only** content is primitive metadata, as §5 found.

`modifierExtension` exists only on backbone elements. On `HumanName`, a
datatype, both layouts ignore it as undescribed content. The transformer logs
"the definitions describe no element of this name". That is FR-016 item 5, not
a case for this question. On a backbone element the new layout keeps it and the
released encoder drops it, so here the new layout is strictly better.

Two engine gaps showed up on the new layout, and neither concerns emptiness.
`name.extension` fails because the engine still looks for the released
encoder's separate `_extension` column, while the new layout stores extensions
inside the element. And naming a field the stored schema does not carry fails
with `FIELD_NOT_FOUND`, which is the Phase 7 tolerance already noted in §5.

### 11. Two more cases the assumption has to name

`scripts/GapProbe.java`, same commit. §10 closes the question for structures
within one conformant document. Two cases lie outside it.

**Positional nulls are written with nothing to align to.** The layout keeps a
repeating primitive's nulls (consequence 1 below), and M1 does not store `_x`.
So the writer emits the placeholders on their own:

```
input:   "given":["Ann",null,"Bee"], "_given":[null,{"extension":[DAR]},null]
written: "given":["Ann",null,"Bee"]

input:   "family":"F", "given":[null], "_given":[{"extension":[DAR]}]
written: "family":"F", "given":[null]
```

FHIR JSON uses `null` in a primitive array only as padding to align it with
`_x`, so neither output is valid FHIR. This is the primitive twin of `{}`: the
same carve-out (FR-016 item 1), the same query parity (§2, §5), and the same
invalid output. The second is exactly what FR-019 calls "an array holding only
nulls". It goes away in M5, when `_given` is written beside it.

**A conformant document can be emptied by its neighbour.** FR-016 item 6 is a
property of the file, not of the document:

```
input:   {"id":"a","photo":[{"contentType":"image/png"},{"size":10}]}
input:   {"id":"b","photo":[{"size":1.5}]}
WARN     Patient.photo.size: the definitions declare a unsignedInt and the
         source carries a double column
written: {"id":"a","photo":[{"contentType":"image/png"},{}]}
written: {"id":"b","photo":[{}]}
```

Document `a` is conformant on its own, and it still comes back with `{}`. Under
the principle below this is emptiness caused by non-conformant content, so it
belongs with the residue. But it means "conformant input" has to mean every
document in the file, not each document taken alone, or the claim that only the
metadata-only case produces `{}` is false.

### 12. Alone in its file, the metadata-only structure had no column

Found by round 5 of the review; fixed by decision 72 after round 6, and
corrected after round 7. §5's `{}` depended on something else in the file
keeping the element's column. The layout is pruned to what the file observes,
and M1 does not store the metadata, so a metadata-only structure that was the
only occurrence of its element had no column. Before and after decision 72,
beside the released encoder (HAPI's parser, then `FhirEncoders.forR4()`):

| Input, alone in the file | Before | After (decision 72) | Released encoder |
| --- | --- | --- | --- |
| `"name":[{"_family":{DAR}}]` | `name` omitted | `name:[{family:null}]`, written `"name":[{}]` | `name` kept |
| `"name":[{"_given":[{DAR},{DAR}]}]` | `name` omitted | `name:[{given:null}]`, written `"name":[{}]` | `name` dropped |
| `"contact":[{"name":{"_family":{DAR},"_given":[{DAR}]}}]` | `contact.name` omitted, and `contact` with it | written `"contact":[{"name":{}}]` | kept, by `_family` |
| `Encounter.period:{"_start":{DAR}}` | `period` omitted | `period` kept, written `{}` | `period` kept |
| `ActivityDefinition.timingTiming:{"_event":[{DAR}]}` (corpus) | omitted, hidden by the harness cascade | kept, written `{}` | dropped |

Each row reports its `_x` as before. For a singular primitive, the "before"
column departed from the released encoder and decision 72 restores parity. For a
repeating one the released encoder is the outlier: HAPI's parser discards an
`_x` array with no `x` array beside it, so the element never reaches the
encoder. The layout follows FHIR, which says the element exists, and T101
records the divergence for M2. Round 7 measured the released encoder's column,
including on the corpus file itself.

The omission was not forced. The file gives `family` no type, but the
definitions do, and the transform already built a null of the declared type for
contradicted content. Decision 72 uses the same null for a primitive observed
only as its `_x`, where the group has the shape FHIR gives it; a malformed group
keeps nothing. `DeferredPruningTest` pins every "after" cell but the corpus one,
which `SpecExampleRoundTripTest` covers now that the cascade is gone. The
"before" column is round 5's and round 6's measurement, except the `contact` row
alone in its file, which round 6 measured only beside a document that kept
`contact`; alone, it follows from the rule that omits an ancestor left empty
(`ResourceTransformerTest.omitsEveryAncestorLeftEmptyBeneathIt`).

## Guiding principle

**Prune a structure according to why it is empty, never according to where it
sits.**

A structure emptied by the carve-out, whose only content was primitive metadata
that M1 does not store, is an element that exists. The spec counts it, the
released encoder counts it where the primitive is singular, and pruning it
changes query answers (§6, §7). It stays, as `{}`, until M5 gives it stored
content, whatever other conformant documents the file holds (§12, decision 72).
Where the primitive repeats, the released encoder drops it, and the layout keeps
it as the spec says (§12).

A structure emptied by non-conformant content — undescribed fields, a
contradicting JSON type (FR-016 item 5), a sibling that re-typed the column
(item 6) — is not an element. Pruning it is correct. It is out of scope for M1,
whose requirement is conformant input. Item 6 reaches across documents, so a
conformant document can be emptied by a non-conformant one in the same file
(§11). The scope has to be stated per file for that reason.

Three consequences follow for any option:

1. **Never compact a primitive value array in storage.** Its nulls are
   positional, the released encoder keeps them, and FHIR aligns them with `_x`
   (decision 63). Output is a different matter. Until M5 writes `_x`, a null
   there has nothing to align with, and the output is invalid (§11).
2. **Build any presence test from source leaves**, with `exists` over arrays, so
   that it is linear in depth. `NestingDepthRoundTripTest` must keep passing.
3. **The answer on the new layout must equal the answer on the released
   encoder** for conformant input, until M5 changes both on purpose. That parity
   is the regression criterion, and §9 says it has to be pinned by our own test.
   Decision 72 removed the exception found for a singular primitive, and records
   the one it keeps on purpose, a repeating primitive the released encoder drops
   (§12).

This also narrows the M2 hazard in T134h. A present but all-null structure
answering `exists()` true is correct where the structure held primitive
metadata. It is wrong only where non-conformant content emptied it.

## Solution options

### A. Defer, and restate the assumption

Leave the transform as it is. The layout keeps the metadata-only item as `{}`.

- **FHIRPath:** parity with the released encoder (§5). No regression.
- **Conformant input:** where every document in the file conforms, the only
  emptiness it can produce is the metadata-only case, which is kept on
  principle. So the M1 requirement is met.
- **Cost in M1:** `io`'s own JSON output writes `{}` for the metadata-only item.
  That output is FR-016 item 1's loss, and it is already reported as a finding.
  It disappears in M5, when `_family` becomes stored content and the structure
  is no longer empty. The same holds for positional nulls written without
  their `_x`, such as `"given":[null]` (§11).
- **Residue:** `{}` from non-conformant input, including a conformant document
  emptied by a neighbour that re-typed a column (§11), and from layout data
  written by another producer, until the custom JSON serde (T134h).
- **Work:** documentation, plus one test. The assumption changes from "the input
  carries nothing that needs pruning" to "no structure is emptied by
  non-conformant content anywhere in the file". The metadata-only structure and
  the unaligned positional null are both named as conformant, expected and
  deliberately kept.

### B. Prune at ingest (the sketch above)

Rejected. It prunes the metadata-only item along with everything else, which is
the regression in §7. Nulling in place instead of dropping does not help.

### B′. Prune at ingest, counting `_x` as presence

The sketch, with the leaf predicate widened to
`storedValue(…).isNotNull() OR source._x.isNotNull()`. It is still built from
source leaves, so it stays linear.

- **FHIRPath:** parity. The DAR item stays `{}`.
- **Non-conformant input:** `maritalStatus: {text: [a, b]}` becomes null, and an
  item that stray content emptied leaves its array. That is decision 66's
  behaviour, on the content it was written for.
- **Temporary part:** only the `_x` term. In M5 `_family` becomes a stored leaf
  and subsumes it. The pruning itself is permanent.
- **Cost:** about 80 lines plus tests, and the spec churn listed under the
  sketch. JSON output in M1 still writes `{}` for the DAR item.
- **For conformant input it stores exactly what A stores.** Everything it buys
  is on input M1 declares out of scope.

### C. Prune at egress only

Leave the layout alone and apply the presence test, built from the stored
leaves, when writing JSON.

- **FHIRPath:** parity, since storage is untouched.
- **Output:** conformant JSON. The DAR item is dropped from the output, which
  shortens the array. That is FR-016 item 1's loss showing up as cardinality in
  the output alone.
- **Coverage:** it also handles layout data written by another producer, which
  B′ does not.
- **Primitive arrays:** it has to settle these too. In M1 it can drop the
  positional nulls from the output, because no `_x` is written for them to align
  with. From M5 it must keep them.
- **Lifetime:** it stops pruning the metadata-only case in M5, when the `_field`
  leaves join the test, and it is thrown away if the serde lands.

## Recommendation

**Take A.** Within the stated scope, a file whose every document is conformant
JSON with no stray fields, A and B′ store identical data, because the metadata-only structure is the only empty
one conformant input can produce and the principle keeps it. B′ spends about 80
lines and a round of spec churn on input M1 does not promise to handle, and the
serde is already the planned home for that input.

Required with A:

1. Restate the assumption in decision 71 and FR-019 as above. This **replaces**
   round 4's second ask, to widen the assumption and call it corpus-true, rather
   than adding to it: the metadata-only case no longer violates the assumption,
   because it is named and kept on principle. The restatement must also name
   the unaligned positional null, and scope conformance to the file (§11).
2. Narrow the M2 hazard in T134h to structures emptied by non-conformant content.
3. Pin the behaviour. In M1, an `io` test asserts that the DAR document keeps
   `{}` in its slot and that `given` keeps its positional nulls. In M2, once T037a exists, the §5 and §7 tables become a
   FHIRPath test over both layouts, so a later pruning that breaks parity fails
   loudly.
4. Round 4's first ask stands: mark FR-019's deferral everywhere it is still
   promised, as listed below.

**When to revisit.** Before M4, not in M1. If the serde will not land before the
public API switches, real-world input with stray fields becomes likely, and B′
earns its cost then.

**If the reviewer will not accept `{}` or unaligned nulls in M1 output,** add C
on top of A rather than switching to B′. C changes only the JSON, not storage or
query answers.

## What round 4 asked for and is still outstanding

Required, if the documentation-only route is taken. The recommendation above
replaces item 2 rather than adding to it:

1. Mark FR-019's deferral everywhere it is still promised —
   `spec.md:102`, `spec.md:297-299`, `data-model.md:195-210`, the three
   exception lists at `spec.md:392-393` / `data-model.md:223` /
   `contracts/storage-layout.md:109`, and the Assumptions section
   `spec.md:653-716` — and extend decision 71's "Consequence for the
   specification".
2. Widen FR-019's statement of the assumption to match decision 71's full one,
   and say at `decisions.md:1991` that the assumption is corpus-true rather than
   conformance-true.

Notes, not required: `FhirJsonReader`'s javadoc lacks the assumption;
`RoundTripHarness.exclude`'s javadoc lacks the caveat; T012's exception
description is incomplete; a plan-linearity assertion would strengthen
`NestingDepthRoundTripTest`; the M2 hazard is recorded only in T134h.

Build state at the time of writing: `6be7377d13`, BUILD SUCCESS, 14:01, 8,434
tests, 0 failures.

## Running the probes

The programs are plain Java against the installed artifacts — no module
build needed.

```bash
D=/tmp/null-probe && mkdir -p $D
mvn -q -o -pl library-api dependency:build-classpath \
  -Dmdep.outputFile=$D/cp.txt -DincludeScope=test
CP=$(cat $D/cp.txt):library-api/target/classes
javac -proc:none -cp "$CP" -d $D/out \
  specs/001-parquet-on-fhir/evidence/scripts/NullProbe.java
java --add-opens=java.base/java.lang=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     -cp "$CP:$D/out" NullProbe
```

`OnlyNull` and `SparkJson` run the same way; `SparkJson` needs only the Spark
jars, not `library-api/target/classes`.

`LayoutProbe`, `SlotProbe`, `MetadataSlotProbe`, `BackboneProbe` and `GapProbe`
also need `io` on the classpath. `LayoutProbe` takes the fixture path; the others take no
arguments:

```bash
mvn -q -o -pl io dependency:build-classpath \
  -Dmdep.outputFile=$D/io-cp.txt -DincludeScope=test
CP=$(cat $D/cp.txt):$(cat $D/io-cp.txt):library-api/target/classes:io/target/classes
javac -proc:none -cp "$CP" -d $D/out \
  specs/001-parquet-on-fhir/evidence/scripts/LayoutProbe.java
java <same --add-opens flags> -cp "$CP:$D/out" LayoutProbe \
  fhirpath/src/test/resources/fhirpath-js/resources/patient-example-2.json
```
