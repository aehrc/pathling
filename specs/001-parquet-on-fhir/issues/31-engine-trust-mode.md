# 31 — The engine runs in `trust`, and conformant data gives correct answers

> **Raised as #2885 (2026-11-09):** the defect the legacy line also has, `toDate()` and `toDateTime()` accepting impossible calendar dates.

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-016, FR-035, FR-063. Decision 133 and its addendum of 2026-10-22; principles 2, 5 and 7, the modes and the exceptions in `evidence/ingress-strictness/README.md`. Evidence: `evidence/engine-tolerance/README.md`.

**What to build:** #2367 delivers one engine mode, `trust`, and handling non-conformant data any other way is the follow-up issue that ticket 26 raises (owner, 2026-10-22). Under `trust` the engine assumes the data it reads is conformant. On data that is not, its behaviour is undefined: a query may fail, or may silently give a wrong answer. This ticket documents that, and fixes what gives a wrong answer on conformant input, which `trust` does not excuse.

What the engine does today on non-conformant data is in the evidence: an invalid stored date fails any comparison, an invalid decimal is absent, a decimal with surrounding whitespace is trimmed, and an all-null structure is counted as present. Under `trust` each of these is allowed, none is a defect, and none is pinned by tests, because undefined behaviour is free to change. `trust` also takes a stored annotation as given, whoever wrote it: whether the engine recomputes one instead belongs to the modes (decision 135), and so to the follow-up.

**In scope, because the input is conformant.**

- **A `dateTime` or `instant` with a time but no timezone** is valid (decision 133's addendum of 2026-10-21), and is compared as today.

**Out of scope: defects the legacy line also has (owner, 2026-11-02).** Under the re-plan, a defect that the legacy (Bunsen) line has too is not fixed here. It is raised by ticket 26 as a GitHub issue labelled `bug`, `PoF_line` and `Bunsen_line`. The first was the conversion functions over a valid string, raised as #2885. If a defect in scope here turns out to be one the legacy line has too, stop and ask the owner whether it moves.

**Blocked by:** None. The decimal criterion that waited on ticket 13 is removed: a decimal beyond `DECIMAL(38,6)` is null on every path, a quantity's value included, so the two paths agree (`QuantityNumericAnnotationTest`).

**Status:** needs-triage

- [ ] The documentation states that the engine assumes conformant data, that its behaviour on non-conformant data is undefined and may be an error or a wrong answer, and what the exceptions accept. It names the `validate` operation, over a source or stored data, as the follow-up that will find such data.
- [ ] The full test suite passes over conformant data.
- [ ] A `dateTime` and an instant with a time but no timezone are pinned by tests as valid.
- [ ] `DateMatcher`'s `ge` ("resource starts at or after parameter start") is checked against the FHIR search specification's overlap reading of `ge`, for example `birthdate=ge2000-06` against a birthDate of `2000`. If main is wrong, it is raised through ticket 26 as a GitHub issue labelled `bug` and `PoF_line` (owner, 2026-11-11, from ticket 43, which kept main's matching and added a plain conjunct on the upper bound).
- [ ] No test pins the engine's behaviour on non-conformant stored data. Existing tests that do are removed or moved to the follow-up. Until ticket 17 stores the metadata group, this includes Pathling's own output: the `{}` and positional-null placeholders of decisions 71 and 72 break level 2, so a test pinning parity on them, such as the data-absent-reason pattern's `name.count()` = 3, is removed too (owner, 2026-10-22).

## Comments

### Narrowed to `trust` (owner, 2026-10-22)

Until 2026-10-22 this ticket built four engine modes for invalid stored values: `strict`, `tolerate`, `repair` and `trust`. The design is kept in `evidence/ingress-strictness/README.md`, under "Modes". The ticket's text before the modes were added is `issues/31-engine-value-tolerance-modes.md` at `cbce542dd5`, on `issue/2367`. It moves, with ticket 33, to the follow-up issue that ticket 26 raises: the decimal grammar in `fhir-schema`, the shared grammar's use in the boundary and decimal decoding, the annotation path, `raise_error` messages naming the resource, the path and the value, the benchmark of parse-free comparison, the ANSI `try_` forms, and the range of `positiveInt` and `unsignedInt`.
