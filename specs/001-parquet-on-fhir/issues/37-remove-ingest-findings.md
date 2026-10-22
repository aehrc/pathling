# 37 — Remove ingest's findings and checks, and list what the layout does not store as exceptions

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-006, FR-016, FR-017, FR-060, FR-063. Decisions 68, 133 (its addendum of 2026-10-22) and 134; "Modes", "Exceptions to the levels" and the XML rows of "Conformance levels" in `evidence/ingress-strictness/README.md`.

**What to build:** Ingest reports nothing (owner, 2026-10-22). #2367 delivers ingest in `trust`, under which behaviour on non-conformant input is undefined, so detecting and reporting it is code and tests that nothing needs. Ingest assumes its input is conformant and checks nothing: where it is not, Spark either fails the read or produces a null, and either is acceptable (owner, 2026-10-22). `ingress-strictness/README.md` leaves the schema check at planning to the follow-up ("It may still check what is free … and the follow-up decides whether it does"); #2367 does not keep it. Content that is valid FHIR but that the layout does not store is not reported either. It is listed as an exception to the guarantee for conformant content, as the Parquet on FHIR specification allows, and dropped silently.

- **The exceptions.** The guarantee of FR-016 is stated for conformant content, less these, each dropped without a finding:
  - `contained` resources (FR-006) and `Bundle` (FR-007), as today;
  - an inline resource declared as abstract `Resource`, such as `Parameters.parameter.resource`;
  - primitive ids and extensions, until ticket 17 stores the metadata group;
  - on a dense schema, content beyond the nesting, extension and open-type bounds (FR-017);
  - `base64Binary` whitespace, and the losses OQ-1's interim answer lists, as today.
- **Delete the findings.** `StrictnessCheck`, `NonConformantContent`, `ResourceTransformer.findings` and `report`, and the warning it logs, are removed. Nothing replaces them.
- **Delete the checks.** The transform no longer compares the observed schema with the definitions. `shapeMismatch`, `isConformingCardinality`, `isConformingMetadataGroup` and `PrimitiveConverter.accepts` go, and the transform casts or extracts each declared element from what Spark inferred as though it conformed. A primitive Spark widened to string is cast to the declared type, so conformant values in the same column survive and a non-conformant one becomes null or fails the read under ANSI; a shape no cast reaches, such as a structure where a primitive is declared or the wrong cardinality, fails the read with Spark's own error. Not every case does either: an integer column one value re-typed to double truncates `1.5` to `1`, and a singular element supplied as an array is stored as the array's text; under `trust` a wrong value is acceptable too (owner, 2026-10-22). Nothing is added to handle any of these. Also removed is what exists only to stand in for a contradicted element: the mismatch branch of the field planning, and the structure branch of `ResourceTransformer.absent` and `absentStructure`. `metadataOnly` and the primitive branch of `absent` stay, because conformant input reaches them, without the `isConformingMetadataGroup` filter. `elementTypeOf`, which `XmlIngestShape` and `XmlEgressShape` also use, moves to wherever the transform keeps it. What conformant input still needs, such as the widenings Spark infers for valid numbers, is decided by the round-trip suites: a check they show conformant input needs stays, covered by a conformant test.
- **The round-trip harness strips the exceptions from the expected side.** `RoundTripHarness` takes from the source JSON what the exceptions list, one test-side rule each, before comparing: `contained`, an inline abstract `Resource`, the `_x` metadata groups until ticket 17, and later the dense bounds. It no longer asks production code what was lost. Its self-test that an unexcluded loss fails the comparison (`failsWhereIgnoredContentIsNotExcluded`) is kept, rewritten to the new mechanism with a listed exception left unstripped, such as a `contained` resource, so that it no longer depends on non-conformant input as `bogusField` does today.
- **Remove the tests that pin behaviour on non-conformant input**, since `trust` leaves it undefined:
  - `StrictnessTest`, all of it. The contained and primitive-metadata cases that remain meaningful are covered by the round-trip suites once the harness strips them, and `dropsPrimitiveMetadataFromTheFittedSchema` moves to `ResourceTransformerTest` if nothing else pins it;
  - in `StructFixtureTransformTest`, `reportsAndNullsAFloatColumnForADecimal`, `reportsAndNullsADateColumnForADate` and `dropsAndReportsAMapWhereAStructureIsDeclared`, and the reporting half of `ordersARepeatingStructureAsTheDefinitionsDeclareAndReportsWhatTheyDoNotDescribe`;
  - in `PrimitiveConverterTest`, `nullsTextSuppliedForABoolean`, `nullsANumberSuppliedForText`, `nullsAnIntegerBeyondTheRangeOfAnInteger`, `nullsAnIntegerColumnRetypedByAFraction`, `nullsTextThatIsNotBase64` and `reportsNothingForConformantEncodings`;
  - in `PrunedSchemaGuaranteeTest`, `excludesContentTheDefinitionsDoNotDescribe`, `excludesEveryValueOfAColumnRetypedByOneOfThem` and `writesAnEmptyObjectWhereIgnoringContentEmptiesAnArrayItem`;
  - in `DeferredPruningTest`, `writesAnEmptyObjectWhereANeighbourRetypedAConformantElementsOnlyContent`: the resource it checks is conformant, but the batch is not, so the outcome is undefined;
  - in `SpecExampleRoundTripTest`, `detectsExactlyTheContentThisLayoutDoesNotStore`. It is the guard that each type's spec examples lose only what is recorded, and the losses it records (`contained`, `Parameters.parameter.resource` and `_x` groups) are the exceptions above, so it is replaced by the stripped round trip over every resource type, not only the selection `roundTripsTheStructuralSubset` covers today;
  - any other assertion on findings, such as those in `IngestFormatRoundTripTest` and `SyntheaRoundTripTest`, which become plain round trips.
- **Keep the tests on conformant input and on failing reads.** Spark's lossless widenings, a repeating element holding one value, base64 whitespace, the deferred pruning of primitive metadata, and every test that malformed JSON or XML fails the read (FR-063, and ticket 32's XML cases) stay.
- **Amend the spec to match.**
  - FR-006: `contained` is not represented, and is dropped silently.
  - FR-016: the exceptions above. Exception 3, content the definitions do not describe or whose JSON type contradicts them, moves out of the list: it is not conformant input, so it is outside the guarantee rather than an exception to it.
  - FR-017: the bounds hold the guarantee within them, and what they drop is an exception, not reported.
  - FR-060: "and report the same findings" and "report" go; the parity is for conformant input.
  - FR-063: ingest is in `trust` too, its behaviour on non-conformant content undefined: it may fail the read, produce a null or store a wrong value. Malformed JSON or XML still fails the read. The interim clause on OQ-1's losses, "reported per column at most", goes.
  - Decision 133's addendum says the follow-up rewrites FR-063. A note records that this ticket rewrites it for ingest now.
  - Decision 134's "reported once per column, as a log line" and the XML row of level 3 ("`contained` is reported", "where JSON logs one per column") are amended. JSON and XML then agree, as FR-060 asks: today XML leaves undescribed content out without a finding while JSON logs one.
  - "Exceptions to the levels" in `ingress-strictness/README.md`: `contained` is dropped with no finding.
  - The level 3 row's `trust` cell in `ingress-strictness/README.md`, "undefined, unless the schema check is kept", becomes "undefined", and the note on checking the schema at planning says #2367 does not.
- **Update the tickets that build on findings.** Ticket 18's bounds reporting (T134f) and its criterion that content dropped by a bound is reported are removed, and so is "with bounds reporting" from its title in the README. Ticket 26's T134e, mis-diagnosed findings, is removed: the inline abstract `Resource` is an exception, and nothing is reported for an empty array. Tickets 16 and 26's follow-up on non-conformant data keep findings as design for the modes beyond `trust`, starting from nothing in the code.

**Out of scope:** findings, modes and a `validate` operation for the modes beyond `trust`, which are the follow-up issue that ticket 26 raises.

**Blocked by:** 32

Ticket 36 also works in `io`, and moving `elementTypeOf` touches `XmlIngestShape` and `XmlEgressShape`; running the two in sequence avoids a conflict.

**Status:** done

- [x] `StrictnessCheck` and `NonConformantContent` are gone, and ingest logs nothing about the content it does not store.
- [x] The transform compares no observed schema with the definitions, and keeps no code that handles a contradicted element.
- [x] The round-trip harness strips the listed exceptions from the expected side, and its self-test still shows, on conformant input, that an unexcluded loss fails the comparison.
- [x] Every resource type's spec examples round-trip with only the listed exceptions stripped.
- [x] No `io` test pins what ingest does with non-conformant input, and the tests listed above are removed or reworked.
- [x] The spec's FR-006, FR-016, FR-017, FR-060 and FR-063, decisions 133 and 134, and the strictness README are amended as described, and tickets 18 and 26 are updated.
- [x] The `io` suite and the round-trip suites pass.
