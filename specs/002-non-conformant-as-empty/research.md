# Research: Non-conformant data reads as empty

Decisions taken with the owner on 2026-11-24 unless stated otherwise.

## R0. One behaviour, no modes

- **Decision**: Conformant data at levels 1 to 4 gives correct results apart from the closed exceptions. Above
  level 1, non-conformant content never fails and is read as empty, silently. Level 1 still fails the read.
- **Rationale**: A load or query that fails on one bad value blocks all analysis. Validation up to level 5 is
  expected to be a separate product, so Pathling does not need to report.
- **Alternatives considered**: #2884's ladder (`strict`, `tolerate`, `repair`, `trust`) with a reporting
  dimension and `validate`; `strict` as an opt-in; findings for what was emptied. All rejected for this
  specification. Each could be added later without changing the default.

## R1. Conversions use the safe forms

- **Decision**: Every conversion, at ingest and in the engine, uses `try_cast`, the other `try_` functions and
  the null-returning parses. The result does not depend on `spark.sql.ansi.enabled`.
- **Rationale**: Spark 4 enables ANSI by default, under which a plain cast fails, contradicting FR-004. The
  harness already runs suites under both settings, so the settings must agree.
- **Alternatives considered**: following the session setting. Rejected because users with ANSI on would get
  failures.

## R2. The decimal grammar is the safe cast

- **Decision**: A decimal is valid where `try_cast` to `SystemDecimal` reads it, as `DecimalNumericKind` does
  today. Forms FHIR's regex forbids but the cast reads (`" 1.5"`, `"+1"`, `"1."`) are a documented divergence
  (FR-020).
- **Rationale**: This matches the cast-semantics rule, adds no per-value regex, and leaves one implementation
  shared by ingest and the engine (FR-013).
- **Alternatives considered**: FHIR regex plus the cast. Rejected as level 4 validation, which FR-009 rules
  out.

## R3. Ingest plans a typed null per column on a mismatch

- **Decision**: The transform compares the type Spark inferred for each element with the declared type, once
  per column. Where a safe cast reaches the declared type, the column is cast. Where none does (an array
  where a singular element is declared, an object where a primitive is declared, a scalar where an array is
  declared), the element is a null of the declared type.
- **Rationale**: Since ticket 37 the transform casts as though the input conformed, so these cases fail the
  read or store the wrong value. A per-column decision is free at run time and needs no new mechanism. It
  brings back ticket 37's removed shape check without findings.
- **Alternatives considered**: decoding each value against its declared type. Rejected as a new ingest
  mechanism, which overlaps #2883 and is much slower.

## R4. Mismatches hidden by a widened column are a divergence

- **Decision**: When a batch mixes conformant and non-conformant rows, Spark widens the column, and the
  per-column plan cannot see individual rows: `["Smith"]` where a string is declared is stored as its JSON
  text, and `1.5` where an integer is declared may be stored as `1`. This is accepted and documented
  (FR-020).
- **Rationale**: Preventing it needs reading with a schema derived from the definitions, which changes the
  interim ingest of decision 134.
- **Alternatives considered**: schema-driven reading now; an open question with a spike. Both deferred. If
  #2883 replaces the interim ingest, it may remove the divergence.

## R5. A shared non-conformant corpus

- **Decision**: One corpus of non-conformant JSON and XML in `io/src/test/resources/nonconformant/`, with a
  test-side writer for directly written PoF Parquet. `io` tests pin what is stored, and `fhirpath` tests pin
  the answers from the same fixtures. `fhirpath` depends on `io`'s main jar at test scope, but `io` publishes no
  test-jar, so `io` gains the `test-jar` goal and `fhirpath` a test-jar dependency on it, as `fhirpath` already
  does for its own tests. Annotated types run with and without annotations.
- **Rationale**: The two modules must agree on every case. Separate fixtures would drift.
- **Alternatives considered**: per-module inline fixtures.

## R6. Tracking

- **Decision**: #2884 is rescoped to this specification, and #2921 is closed as folded into it. Branch
  `issue/2884`, one PR per story or small group of stories, into `release/11.0.0`.
- **Alternatives considered**: a new umbrella issue; keeping both issues.

## R7. The array rule is mostly in place in the engine

- **Finding**: `DefaultRepresentation.isElement` already treats a position of a primitive array as an element
  only where the value or its id or extensions are present, and leaves out the others. FR-007 in the engine
  therefore needs tests that pin it, not new code. On the ingest side, `Annotator.repeatingGroup` pairs over
  the values' length, and FR-008 changes it to the longer array.

## Open

### OQ-A. Bottom-up structure emptiness

- **Target**: a structure is absent when every field is absent after the rules, recursively.
- **Spike**: implement the predicate for the traversed paths only, then measure the plan size and query time
  on the deepest and widest resource types of the benchmark corpus, with `NestingDepthRoundTripTest` as the
  depth check. Record the results in `evidence/oq-a-structure-emptiness.md`.
- **Fallbacks**: shallow (a literal `{}` or all-null structure only); computed at ingest only, with the engine
  trusting stored structures; a stored presence annotation; structures left as stored.

### OQ-B. Parquet and Delta conformance, and stored annotations

- **Cases**: a column of another type, a missing field, an extra field, a malformed metadata group, and a
  stored annotation that is null or disagrees with its value.
- **Options**: per field, cast where possible and absent otherwise, decided at planning; stricter, with the
  dataset accepted as the layout or refused at planning; absent without casting. Whether stored annotations
  are trusted or recomputed is part of the decision (decision 135 of spec 001).
- **Spike**: catalogue what each case does today, and the cost of recomputing annotations on the benchmark.
  Record the results in `evidence/oq-b-parquet-conformance.md`.

### OQ-C. Where an invalid annotated value becomes empty

- **Options**: (a) only for operations that parse it; (b) absent everywhere, with the engine parsing every
  annotated value on traversal; (c) at the conversion from the FHIR type to the System type (the owner's least
  preferred).
- **Spike**: list the operations that cross the FHIR-to-System boundary and those that do not, and measure the
  cost of (b) and (c) on the benchmark. Record the results in `evidence/oq-c-invalid-annotated-values.md`.
