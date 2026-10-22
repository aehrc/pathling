# 31 — The engine runs in `trust`, and conformant data gives correct answers

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-016, FR-035, FR-063. Decision 133 and its addendum of 2026-10-22; principles 2, 5 and 7, the modes and the exceptions in `evidence/ingress-strictness/README.md`. Evidence: `evidence/engine-tolerance/README.md`.

**What to build:** #2367 delivers one engine mode, `trust`, and handling non-conformant data any other way is the follow-up issue that ticket 26 raises (owner, 2026-10-22). Under `trust` the engine assumes the data it reads is conformant. On data that is not, its behaviour is undefined: a query may fail, or may silently give a wrong answer. This ticket documents that, and fixes what gives a wrong answer on conformant input, which `trust` does not excuse.

What the engine does today on non-conformant data is in the evidence: an invalid stored date fails any comparison, an invalid decimal is absent, a decimal with surrounding whitespace is trimmed, and an all-null structure is counted as present. Under `trust` each of these is allowed, none is a defect, and none is pinned by tests, because undefined behaviour is free to change. `trust` also takes a stored annotation as given, whoever wrote it: whether the engine recomputes one instead belongs to the modes (decision 135), and so to the follow-up.

**In scope, because the input is conformant.**

- **Conversion functions over a valid string.** `toDate()` and `toDateTime()` check only the pattern, so `'2020-02-30'.toDate()` returns `2020-02-30` rather than empty. Their input is a valid `String`, so this is a FHIRPath defect, not a tolerance question. They check the calendar as FHIR's grammar does, which lives in `fhir-schema` (owner, 2026-10-21).
- **A valid decimal beyond `DECIMAL(32,6)`.** A decimal of 29 or 40 digits is valid FHIR. It reads as empty on the decimal path, yet `valueQuantity > 1 'mg'` is true for it, so the two paths disagree on conformant data. Loss of precision is an accepted exception ("Exceptions to the levels"); a value that becomes empty is not. How it is resolved is decided when the ticket is picked up (owner, 2026-10-21). Stop and ask the owner before changing the behaviour.
- **A `dateTime` or `instant` with a time but no timezone** is valid (decision 133's addendum of 2026-10-21), and is compared as today.

**The date grammar is shared with ticket 14 (decision 135).** The range annotation kind, in `fhir-schema`, parses dates with the same grammar this ticket puts there. Whichever ticket lands first puts it in place, and the other uses it.

**Blocked by:** 13, for the decimal criterion only, since its annotation changes how the decimal path reads a value. The rest can start at any time.

**Status:** needs-triage

- [ ] The documentation states that the engine assumes conformant data, that its behaviour on non-conformant data is undefined and may be an error or a wrong answer, and what the exceptions accept. It names the `validate` operation, over a source or stored data, as the follow-up that will find such data.
- [ ] The full test suite passes over conformant data.
- [ ] `toDate()` and `toDateTime()` return empty for `2020-02-30`, `2020-13-45` and `…T25:00:00Z`, using the grammar in `fhir-schema`.
- [ ] The decimal and Quantity paths agree on a valid decimal beyond `DECIMAL(32,6)`, as the owner decides at pickup, and the case is pinned by tests.
- [ ] A `dateTime` and an instant with a time but no timezone are pinned by tests as valid.
- [ ] No test pins the engine's behaviour on non-conformant stored data. Existing tests that do are removed or moved to the follow-up. Until ticket 17 stores the metadata group, this includes Pathling's own output: the `{}` and positional-null placeholders of decisions 71 and 72 break level 2, so a test pinning parity on them, such as the data-absent-reason pattern's `name.count()` = 3, is removed too (owner, 2026-10-22).

## Comments

### Narrowed to `trust` (owner, 2026-10-22)

Until 2026-10-22 this ticket built four engine modes for invalid stored values: `strict`, `tolerate`, `repair` and `trust`. The design is kept in `evidence/ingress-strictness/README.md`, under "Modes". The ticket's text before the modes were added is `issues/31-engine-value-tolerance-modes.md` at `cbce542dd5`, on `issue/2367`. It moves, with ticket 33, to the follow-up issue that ticket 26 raises: the decimal grammar in `fhir-schema`, the shared grammar's use in the boundary and decimal decoding, the annotation path, `raise_error` messages naming the resource, the path and the value, the benchmark of parse-free comparison, the ANSI `try_` forms, and the range of `positiveInt` and `unsignedInt`.
