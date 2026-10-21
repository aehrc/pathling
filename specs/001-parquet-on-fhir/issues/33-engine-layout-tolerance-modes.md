# 33 — The engine handles non-conformant layout shapes in the same three modes

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-016, FR-019, FR-023, FR-063. Decisions 71, 72 and 133; principle 1 in `evidence/null-and-empty-handling.md`, "Guiding principle". Evidence: `evidence/engine-tolerance/README.md`, "Layout". Ticket 31 defines the modes; this ticket applies them to shapes.

**What to build:** A defined engine behaviour for stored shapes that FHIR JSON forbids, under the `strict`, `tolerant` and `trust` modes of ticket 31. A Parquet file in a conformant schema can still hold them, and today the engine gives each one an answer that depends on the shape rather than on a rule.

The shapes, and what the engine does with each today (measured, see the evidence):

| Shape | Today |
|---|---|
| An empty array, `[]` | absent |
| A null item in a complex array | removed when traversed |
| A structure whose every field is null, singular or repeating | present: `exists()` true, counted, `forEach` gives a row, `where(x.empty())` matches it |
| A positional null in a primitive array with nothing in its metadata group to align with | counted: `given = [null, Ann]` gives `count()` 2 and `first()` empty |
| A positional null aligned with metadata | counted, which is correct |

Empty strings are values, and are ticket 31's.

**The modes.**

| Mode | A non-conformant shape |
|---|---|
| `strict` | The query fails, naming the resource id and the path. |
| `tolerant` | Normalised to what FHIR means by it while traversing: `[]` and a null item in a complex array are absent, a structure with no content is absent, and an unaligned positional null is removed from its array. |
| `trust` | Taken as stored. This is today's behaviour. |

`tolerant` normalises from the leaves up: a structure has no content when every field is absent after normalisation, so `{family: ""}` has none either. It is computed only for the paths a query traverses, and the check for a structure reads only the fields beneath it.

**Before ticket 17 this conflicts with decisions 71 and 72.** Pathling's own output relies on two of these shapes as placeholders for primitive metadata it does not yet store: a structure whose only content was a primitive's id or extensions is kept as `{}`, and a repeating primitive's positional nulls are kept. Normalising them changes `exists()` and `count()` on conformant input, such as the data-absent-reason pattern, and breaks the parity with the released encoder that the guiding principle requires. Once the metadata group is stored, each placeholder has non-null content, `tolerant` leaves it alone, and an empty structure or an unaligned null has no legitimate meaning whoever wrote it. That is why this ticket waits for 17.

**Out of scope.**

- Export. FR-019's pruning of empty structures and null-only arrays on output is deferred with OQ-1. It should use the same predicate as `tolerant` when it lands, so that export and the engine agree on what is empty.
- A schema that does not conform: a wrong column type, a field name that differs only in case, a metadata group of the wrong shape. This is not yet ticketed.
- Ingest. Decision 133 makes these shapes invalid at ingest; ticket 16 implements it.

**Blocked by:** 17, 31.

**Status:** needs-triage

- [ ] In `tolerant`, each shape in the table is normalised as described, pinned by tests on a singular and a repeating element, at the root and nested, over the PoF layout written directly.
- [ ] In `strict`, each shape fails the query, naming the resource id and the path.
- [ ] In `trust`, each shape behaves as it does today, and the tests pin that.
- [ ] A structure or positional null that carries stored metadata is not normalised in any mode. The data-absent-reason document keeps `name.count()` = 3 (`evidence/null-and-empty-handling.md`, §6).
- [ ] The full test suite gives identical results in every mode over conformant data.
- [ ] The cost of the structure check is measured on the deepest and widest resource types of the benchmark corpus, and `NestingDepthRoundTripTest` still passes.
- [ ] The documentation states what each mode does with each shape.
