# Feature Specification: Non-conformant data reads as empty

**Feature**: `002-non-conformant-as-empty`
**Created**: 2026-11-24
**Status**: Draft
**Rescopes**: [#2921](https://github.com/aehrc/pathling/issues/2921) (Run the engine in trust mode so
conformant data gives correct answers) and [#2884](https://github.com/aehrc/pathling/issues/2884) (Handle
non-conformant data beyond trust: modes, validate and schema checks).

## Background

`specs/001-parquet-on-fhir` left ingest and the engine in `trust`: on data that does not conform, behaviour
is undefined, which may mean an error or a wrong answer (FR-063 there). #2884 planned a ladder of modes
(`strict`, `tolerate`, `repair`), a reporting dimension, a `validate` operation and ingest presets on top of
it, following `specs/001-parquet-on-fhir/evidence/ingress-strictness/README.md`.

This specification replaces both with **one behaviour and no modes** (owner, 2026-11-24):

- **Conformant data gives correct results.** The property "No loss on conformant input" holds: for
  input that conforms at levels 1 to 4 (container format, FHIR encoding, structure, value), ingest and the engine lose nothing and give correct answers, apart from a closed list
  of exceptions. Level 5 (bindings, minimum cardinality, invariants, profiles) is not part of conformance:
  data breaking only level 5 is conformant.
- **Non-conformant data above level 1 never fails.** Content that cannot be read correctly is treated as if
  it were absent from the input. Content that can be converted to the declared type is converted, with
  Spark's non-ANSI cast semantics: a conversion that cannot succeed gives null.
- **Nothing is checked that nothing needs.** No level 4 validation is done as such. A value's format is
  examined only where something must operate on it: computing an annotation at ingest, or an engine
  operation that needs the value's type, such as a comparison.
- **Silent.** There are no findings, warnings, modes or validation. Validation up to level 5 is expected to
  be a separate product.

The conformance levels, the kinds of primitive and the properties referred to throughout are those of
[`docs/conformance.md`](../../docs/conformance.md).

## User Scenarios & Testing _(mandatory)_

### User Story 1 - Conformant data gives correct answers, and the behaviour is documented (Priority: P1)

An analyst loads FHIR data that conforms at levels 1 to 4 and queries it. Every answer is correct, apart
from the documented exceptions. The documentation states the rule for conformant and non-conformant data,
so the analyst knows what to expect from data of uncertain quality without reading the code.

**Why this priority**: It is the guarantee every other story is measured against, and the part of #2921
that remains. Without the documentation, users of non-conformant data cannot tell an answer from an
accident.

**Independent Test**: Run the full test suite over conformant data, and the new timezone tests; read the
documentation page.

**Acceptance Scenarios**:

1. **Given** a `dateTime` and an `instant` with a time but no timezone, **When** they are ingested and
   compared, **Then** both are stored unchanged and compared as today, as valid values.
2. **Given** conformant data in every test suite, **When** the suites run, **Then** they pass.
3. **Given** the documentation, **When** a user reads it, **Then** it states that conformant data gives
   correct results except for the listed exceptions, that non-conformant content above level 1 is read as
   empty without failing or reporting, that level 1 still fails the read, and that validation is not
   provided.

---

### User Story 2 - Non-conformant JSON and XML load without failing (Priority: P1)

A data engineer ingests an extract from a system that produces imperfect FHIR JSON or XML: empty arrays
and objects, values of the wrong JSON type, an array where a single value is declared, invalid dates. The
load succeeds. What can be converted is converted, and everything else is absent from the stored data.

**Why this priority**: A load that fails on one bad value in millions blocks all analysis of an extract.
Real extracts routinely carry such content.

**Independent Test**: Ingest fixtures holding each non-conformant shape from the table under FR-004 to
FR-008, in JSON and in XML, and check the stored data and the query answers over it.

**Acceptance Scenarios**:

1. **Given** JSON with `null`, `[]`, `{}` or `""` as an element's value, **When** it is ingested, **Then**
   the load succeeds and the element is absent.
2. **Given** `"family": 123` where a string is declared, or `"5"` where an integer is declared, **When** it
   is ingested, **Then** the value is stored converted, as `"123"` and `5`.
3. **Given** `"abc"` where an integer is declared, **When** it is ingested, **Then** the value is absent.
4. **Given** an array where a singular element is declared, or an object where an array is declared,
   **When** it is ingested, **Then** the element is absent.
5. **Given** a resource that is not well-formed JSON or XML, **When** it is ingested, **Then** the read
   fails, as today.
6. **Given** the same content in JSON and in XML, **When** each is ingested, **Then** the stored data is
   the same.

---

### User Story 3 - Arrays keep their valid items (Priority: P1)

A repeating element holds some valid and some invalid items. The invalid items drop out, and the valid
ones are counted and returned as if the invalid ones had never been there. A null that carries an id or
extensions in its metadata is valid FHIR and is kept in place.

**Why this priority**: Repeating elements are where one bad item would otherwise skew `count()`, `first()`
and every row `forEach` produces.

**Independent Test**: Query arrays mixing valid items, invalid items and nulls, with and without paired
metadata, after ingest and when stored directly.

**Acceptance Scenarios**:

1. **Given** `given: ["Ann", <invalid>, "Bo"]` with no `_given`, **When** it is queried, **Then**
   `given.count()` is 2 and `given.first()` is `"Ann"`.
2. **Given** `given: ["Ann", null]` with `_given: [null, {"extension": [...]}]`, **When** it is queried,
   **Then** the second item is kept in place with its extension, as today.
3. **Given** a stored primitive array `[null, "Ann"]` with no metadata group, **When** it is queried,
   **Then** `count()` is 1 and `first()` is `"Ann"`.
4. **Given** a complex array holding a null item, **When** it is traversed, **Then** the null is not an
   item.
5. **Given** more `_given` groups than `given` values, **When** it is ingested, **Then** every group is kept,
   paired with a null value at its position.

---

### User Story 4 - Stored data from any writer never fails a query (Priority: P1)

An analyst queries a Parquet or Delta dataset in the PoF layout written by another tool. Some values are
not valid for their type: an impossible date, a decimal that is not a number. Queries do not fail. An
operation that needs a value's type treats an invalid value as empty.

**Why this priority**: A Parquet on FHIR dataset cannot be trusted any more than JSON can. Today an invalid
stored date fails every comparison with an error naming neither the resource nor the path.

**Independent Test**: Write PoF data directly with invalid values of every annotated type, with and without
annotations, and run comparisons, arithmetic and conversions over it.

**Acceptance Scenarios**:

1. **Given** a stored `birthDate` of `2020-02-30`, **When** `birthDate > @2000-01-01` is evaluated, **Then**
   the result is empty and the query succeeds.
2. **Given** a stored decimal `abc`, **When** it is compared or used in arithmetic, **Then** the result is
   empty and the query succeeds.
3. **Given** the same invalid values with stored annotations and without them, **When** the same queries
   run, **Then** the answers are identical.
4. **Given** an invalid value of an annotated type, **When** an operation that does not need its type
   reads it, such as `exists()` or selecting the element, **Then** the answer is the one OQ-C settles.

---

### User Story 5 - Structures left with no content are absent (Priority: P2)

A structure whose content is all absent, either because it was stored as `{}` or because every value in it
was invalid, is itself absent. `name: [{"family": ""}]` has no name.

**Why this priority**: It completes the "as if absent" rule for structures, but whether it can be done at
an acceptable cost is open (OQ-A), so it follows the stories that are settled.

**Independent Test**: Query structures emptied at the root and nested, singular and repeating, stored
directly and ingested, and measure the cost on the deepest and widest resource types of the benchmark
corpus.

**Acceptance Scenarios**:

1. **Given** `maritalStatus: {}` stored directly, **When** `maritalStatus.exists()` is evaluated, **Then** it
   is false.
2. **Given** `name: [{"family": ""}, {"family": "Smith"}]`, **When** `name.count()` is evaluated, **Then** it
   is 1, and `forEach name` gives one row.
3. **Given** a structure whose only content is a primitive's id or extension, **When** it is queried,
   **Then** it is present.

---

### User Story 6 - Parquet whose schema or annotations do not conform (Priority: P3)

An analyst reads a Parquet or Delta dataset whose schema departs from the layout the definitions derive, or
whose stored annotations disagree with their values. The behaviour is the one OQ-B settles, and is
documented.

**Why this priority**: Only datasets from other producers, or merged from differently written sources,
carry these cases, and the right behaviour is not decided.

**Independent Test**: Read datasets with a retyped column, a missing field, an extra field, a malformed
metadata group and a disagreeing annotation, and check the outcome against OQ-B's decision.

**Acceptance Scenarios**:

1. **Given** each schema departure listed in OQ-B, **When** the dataset is read and queried, **Then** the
   outcome is the one OQ-B's decision records, and is the same for every query.
2. **Given** a null `__numeric` beside a present decimal, **When** the decimal is compared, **Then** the
   outcome is the one OQ-B's decision records.

### Edge Cases

- A `null` in a primitive array that its `_x` group pairs is conformant and kept. One with no paired
  metadata is removed (FR-007).
- `given: []` beside a `_given` holding groups: the groups are kept as positional nulls (FR-008).
- `" 1.5"` as a decimal is read as `1.5`, because the cast trims (a documented divergence, FR-020). A date
  with surrounding whitespace does not parse and is invalid.
- In a batch mixing `family: "Smith"` and `family: ["Smith"]`, the second may be stored as the text
  `["Smith"]` rather than absent (a documented divergence, FR-020).
- A converted value need not keep its exact text: `"family": 1.50` may be stored as `"1.5"`. Non-conformant
  input is outside the guarantee (FR-005).
- A text-only type (`string`, `code`, `id`, `uri` and the like) that breaks its regex is not checked and is
  kept as received. Only `""` is absent (FR-004, FR-009).
- An element the definitions do not describe is left out, as today.
- `contained` and `Bundle` are exceptions, not non-conformant content (FR-002).
- A decimal beyond `DECIMAL(38,6)` is valid FHIR but null at query time, as FR-035 of spec 001 records; it
  is not affected by this specification.
- The fixture `fhirpath-js/resources/quantity-example.json` carries `QuestionnaireResponse.item.answer` as
  an object where an array is declared. Under FR-006 its `answer` is absent, so the fixture is used again,
  with only the cases that expect the object read as an array still excluded (FR-015).

## Requirements _(mandatory)_

### Conformant data

- **FR-001**: For input conforming at levels 1 to 4, ingest MUST store, and the engine MUST answer, as
  FR-016 of spec 001 requires, apart from the exceptions of FR-002. Data breaking only level 5 constraints
  is conformant.
- **FR-002**: The exceptions MUST be a closed list, kept in one place in the documentation: `contained`
  resources and `Bundle`; an inline resource held by an element declared as the abstract `Resource`; on a
  dense schema, content beyond its bounds; `base64Binary` whitespace; byte identity (key order,
  whitespace, escaping and number formatting); and the decimal precision losses spec 001's OQ-1 lists until
  it is answered.
- **FR-003**: A `dateTime` or `instant` with a time but no timezone MUST be accepted as valid, by ingest and
  the engine, and compared as today.

### Non-conformant content

- **FR-004**: Above level 1, ingest and the engine MUST NOT fail on non-conformant content. In JSON, `null`
  (except as positional padding, FR-007), `[]`, `{}` and `""` MUST be read as absent, for every type. XML
  content that FHIR XML forbids is handled as decided in spec 001 (the XML rows of its conformance levels).
- **FR-005**: A value whose JSON type differs from the one the definitions declare MUST be converted to the
  declared type where a non-ANSI Spark cast converts it, and MUST be absent where it does not. The
  converted value need not keep the input's exact text.
- **FR-006**: An element whose cardinality differs from the definitions (an array where a singular element
  is declared, a single value or object where an array is declared, an object where a primitive is
  declared) MUST be absent. It MUST NOT be wrapped or unwrapped.
- **FR-007**: In a primitive array, an item that is absent under FR-004 to FR-006, or under OQ-C's decision, MUST be removed
  unless the paired metadata group carries an id or extensions at its position, in which case it MUST be
  kept as a positional null with its metadata. In a complex array, a null item MUST be removed. This holds
  for data ingested and for data stored by any writer.
- **FR-008**: Pairing a primitive array with its `_x` metadata array MUST run over the longer of the two, so
  that a metadata group with no value at its position is kept as a positional null with that metadata.
- **FR-009**: Neither ingest nor the engine MUST validate a value's lexical form as such (level 4). Text-only
  types are stored and read as received.
- **FR-010**: Where ingest computes an annotation and the value does not parse with the shared grammar,
  ingest MUST store the value's text as received with a null annotation, and MUST NOT fail.
- **FR-011**: Where an engine operation needs a value's type (comparison, arithmetic, conversion functions,
  a quantity's canonical form, and any other operation that parses the stored text) and the value does not
  parse with the shared grammar, the operation MUST treat the value as empty and MUST NOT fail.
- **FR-012**: An invalid value of an annotated type MUST give the same answer to every query with its
  annotation stored and without it. How it answers operations that do not need its type is settled by
  OQ-C.
- **FR-013**: Ingest and the engine MUST parse dates, times and decimals with one grammar, shared by both,
  so that a value invalid to one is invalid to the other.
- **FR-014**: A structure whose every field is absent, after applying FR-004 to FR-011 from the leaves up,
  MUST be read as absent by the engine, and ingest MUST NOT store it as present. This is subject to OQ-A.
- **FR-015**: Ingest and the engine MUST NOT report non-conformant content: no findings, warnings, or log
  lines about it. Tests MUST pin the behaviour of FR-004 to FR-014, replacing #2921's requirement that no
  test pin behaviour on non-conformant data. Conformance cases excluded only because their fixture is
  non-conformant, such as those of `quantity-example.json`, MUST run again, excluded only where they expect
  a behaviour this specification rules out.

### Stored datasets

- **FR-016**: The rules of FR-004 to FR-014 MUST apply to Parquet and Delta data in the PoF layout read by
  the engine, whoever wrote it.
- **FR-017**: A dataset whose schema departs from the layout the definitions derive, and a stored
  annotation that disagrees with its value, MUST be handled as OQ-B settles, and the behaviour MUST be
  documented.

### Specification and documentation

- **FR-018**: Spec 001's FR-063 MUST be rewritten to this rule, and its FR-016 exception 3 (primitive
  metadata behind an ingest option) removed now that primitive metadata is always stored.
- **FR-019**: The documentation MUST state the rule of FR-001 to FR-017 for users, with an example of each
  kind of non-conformant content and what it reads as.
- **FR-020**: The documentation MUST list, as divergences from the FHIR specification, the places where
  non-conformant content is read as a value rather than as empty (owner, 2026-11-24):
    1. a decimal is valid where the safe cast reads it, so forms FHIR's regex forbids, such as `" 1.5"`,
       `"+1"` and `"1."`, are read as numbers;
    2. a converted value does not keep its exact text (FR-005);
    3. in a batch where some rows conform and others do not, a mismatch hidden by the column type the batch
       infers is read as that type: an array where a string is declared is stored as the array's JSON text,
       and `1.5` where an integer is declared may be stored as `1`. The answer then depends on the rest of
       the batch, which the property "Same input, same outcome" of `docs/conformance.md` forbids for conformant
       input only.

### Open questions

Each is settled by a spike that records its measurements under `evidence/`, then an owner decision recorded
in `research.md`, before the story it gates is built.

- **OQ-A: Can bottom-up emptiness of structures be implemented efficiently?** Gates US5 and FR-014. Target:
  bottom-up, as FR-014 states. Fallbacks if it cannot: emptiness of a literal `{}` or all-null structure
  only (shallow); computed at ingest only, with the engine trusting structures as stored; a presence
  annotation stored beside each structure; or structures left as stored.
- **OQ-B: How are Parquet and Delta data held to the layout?** Gates US6 and FR-017. Covers a column of
  another type, a missing field, an extra field, a malformed metadata group, and a stored annotation that
  is null or disagrees with its value (decision 135). Options: per field, cast where possible and absent
  otherwise, decided once at planning; stricter for Parquet, with the dataset either accepted as the layout
  or refused at planning; or absent without casting. Whether stored annotations are trusted or recomputed is
  part of the same decision.
- **OQ-C: Where does an invalid value of an annotated type become empty?** Gates US4 scenario 4 and the
  second half of FR-012. Options: (a) only for operations that parse it, so `exists()` is true and the text
  is selectable; (b) absent everywhere, with ingest emptying it and the engine parsing every value of an
  annotated type on every traversal; (c) at the conversion from the FHIR type to the System type, wherever
  the engine crosses it (the owner's least preferred).

### Out of scope

- Validation, at ingest or over stored data, and the `validate` operation.
- Findings, warnings and any reporting of non-conformant content.
- Modes (`strict`, `tolerate`, `repair`, `trust`), repair levels and invalid-value policies.
- Wrapping a single value into an array or unwrapping a one-item array.
- Storing content the definitions do not describe (ticket 36's D1 and D2).
- Level 4 validation of text-only types.
- Level 5 constraints.

### Key Entities

- **Conformance level**: the first rule a piece of content breaks, from 1 (container format) to 5 (valid
  FHIR), as `docs/conformance.md` defines them.
- **Exception**: conformant content the layout does not store, dropped silently, from the closed list of
  FR-002.
- **Shared grammar**: the one definition of the lexical forms of dates, times and decimals, used by ingest
  and the engine alike.
- **Positional null**: a null item in a primitive array, kept when its metadata group carries content at
  the same position, removed otherwise.

## Success Criteria _(mandatory)_

### Measurable Outcomes

- **SC-001**: Every test suite passes over conformant data, with no test excluded for a reason this
  specification covers.
- **SC-002**: Every non-conformant case in the fixtures, above level 1, loads and queries without an error,
  in JSON, XML and directly written Parquet.
- **SC-003**: For every invalid value of an annotated type, the answers with annotations stored and without
  them are identical.
- **SC-004**: The same content ingested from JSON and from XML stores the same data.
- **SC-005**: Ingest throughput on conformant input, and query time on the benchmark, are within the
  benchmark's noise of the build before this feature. Any structure emptiness check (US5) is measured on the
  deepest and widest resource types of the benchmark corpus.
- **SC-006**: Each of OQ-A, OQ-B and OQ-C has a recorded decision backed by evidence before the story it
  gates is built.

## Assumptions

- The conformance levels of `docs/conformance.md`, and the exceptions and XML rules of
  `specs/001-parquet-on-fhir`, stand except where this specification overrides them.
- Primitive metadata is always stored and read (spec 001, tickets 17, 52 and 54), so a positional null can
  be judged by its metadata.
- The interim ingest of decision 134 remains the mechanism. If #2883 replaces it, the replacement meets
  these requirements too.
- Data written by Pathling 9.9.0 or the legacy layout is out of scope: the legacy layout is refused at read
  time (spec 001, FR-037).
- Users who need to know what was dropped will use a separate validation product.
