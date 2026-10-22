# 37 — Remove ingest's findings, and list what the layout does not store as exceptions

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-006, FR-016, FR-017, FR-060, FR-063. Decisions 68, 133 (its addendum of 2026-10-22) and 134; "Modes", "Exceptions to the levels" and the XML rows of "Conformance levels" in `evidence/ingress-strictness/README.md`.

**What to build:** Ingest reports nothing (owner, 2026-10-22). #2367 delivers ingest in `trust`, under which behaviour on non-conformant input is undefined, so detecting and reporting it is code and tests that nothing needs. Content that is valid FHIR but that the layout does not store is not reported either. It is listed as an exception to the guarantee for conformant content, as the Parquet on FHIR specification allows, and dropped silently.

- **The exceptions.** The guarantee of FR-016 is stated for conformant content, less these, each dropped without a finding:
  - `contained` resources (FR-006) and `Bundle` (FR-007), as today;
  - an inline resource declared as abstract `Resource`, such as `Parameters.parameter.resource`;
  - primitive ids and extensions, until ticket 17 stores the metadata group;
  - on a dense schema, content beyond the nesting, extension and open-type bounds (FR-017);
  - `base64Binary` whitespace, and the losses OQ-1's interim answer lists, as today.
- **Delete the findings.** `StrictnessCheck`, `NonConformantContent`, `ResourceTransformer.findings` and `report`, and the warning it logs, are removed. Nothing replaces them.
- **Keep the silent ignore.** The transform still decides from the observed schema whether a column has the shape and encoding the definitions declare, and stores it only where it does: the boolean outcome of `shapeMismatch`, `isConformingCardinality`, `isConformingMetadataGroup` and `PrimitiveConverter.accepts`. These move to wherever the transform keeps them, along with `elementTypeOf`, which `XmlIngestShape` and `XmlEgressShape` also use. Their message text goes. So does what exists only to keep a typed null in place after a contradiction: the structure branch of `ResourceTransformer.absent` and `absentStructure`, where a contradicted structure now simply does not appear. The primitive branch stays, because `metadataOnly` needs it for conformant input.
- **The round-trip harness strips the exceptions from the expected side.** `RoundTripHarness` takes from the source JSON what the exceptions list, one test-side rule each, before comparing: `contained`, an inline abstract `Resource`, the `_x` metadata groups until ticket 17, and later the dense bounds. It no longer asks production code what was lost. Its self-test that an unexcluded loss fails the comparison (`failsWhereIgnoredContentIsNotExcluded`) is kept, rewritten to the new mechanism.
- **Remove the tests that pin behaviour on non-conformant input**, since `trust` leaves it undefined:
  - `StrictnessTest`, all of it. The contained and primitive-metadata cases that remain meaningful are covered by the round-trip suites once the harness strips them, and `dropsPrimitiveMetadataFromTheFittedSchema` moves to `ResourceTransformerTest` if nothing else pins it;
  - in `StructFixtureTransformTest`, `reportsAndNullsAFloatColumnForADecimal`, `reportsAndNullsADateColumnForADate` and `dropsAndReportsAMapWhereAStructureIsDeclared`, and the reporting half of `ordersARepeatingStructureAsTheDefinitionsDeclareAndReportsWhatTheyDoNotDescribe`;
  - in `PrimitiveConverterTest`, `nullsTextSuppliedForABoolean`, `nullsANumberSuppliedForText`, `nullsAnIntegerBeyondTheRangeOfAnInteger`, `nullsAnIntegerColumnRetypedByAFraction`, `nullsTextThatIsNotBase64` and `reportsNothingForConformantEncodings`;
  - in `PrunedSchemaGuaranteeTest`, `excludesContentTheDefinitionsDoNotDescribe`, `excludesEveryValueOfAColumnRetypedByOneOfThem` and `writesAnEmptyObjectWhereIgnoringContentEmptiesAnArrayItem`;
  - in `DeferredPruningTest`, `writesAnEmptyObjectWhereANeighbourRetypedAConformantElementsOnlyContent`: the resource it checks is conformant, but the batch is not, so the outcome is undefined;
  - any other assertion on findings, such as those in `IngestFormatRoundTripTest` and `SyntheaRoundTripTest`, which become plain round trips.
- **Keep the tests on conformant input and on failing reads.** Spark's lossless widenings, a repeating element holding one value, base64 whitespace, the deferred pruning of primitive metadata, and every test that malformed JSON or XML fails the read (FR-063, and ticket 32's XML cases) stay.
- **Amend the spec to match.**
  - FR-006: `contained` is not represented, and is dropped silently.
  - FR-016: the exceptions above. Exception 3, content the definitions do not describe or whose JSON type contradicts them, moves out of the list: it is not conformant input, so it is outside the guarantee rather than an exception to it.
  - FR-017: the bounds hold the guarantee within them, and what they drop is an exception, not reported.
  - FR-063: ingest is in `trust` too, its behaviour on non-conformant content undefined. Malformed JSON or XML still fails the read.
  - Decision 134's "reported once per column, as a log line" and the XML row of level 3 ("`contained` is reported", "where JSON logs one per column") are amended. JSON and XML then agree, as FR-060 asks: today XML leaves undescribed content out without a finding while JSON logs one.
  - "Exceptions to the levels" in `ingress-strictness/README.md`: `contained` is dropped with no finding.
- **Update the tickets that build on findings.** Ticket 18's bounds reporting (T134f) and its criterion that content dropped by a bound is reported are removed. Ticket 26's T134e, mis-diagnosed findings, is removed: the inline abstract `Resource` is an exception, and nothing is reported for an empty array. Tickets 16 and 26's follow-up on non-conformant data keep findings as design for the modes beyond `trust`, starting from nothing in the code.

**Out of scope:** findings, modes and a `validate` operation for the modes beyond `trust`, which are the follow-up issue that ticket 26 raises.

**Blocked by:** 32

**Status:** ready-for-agent

- [ ] `StrictnessCheck` and `NonConformantContent` are gone, and ingest logs nothing about the content it does not store.
- [ ] The transform still stores a column only where its shape and encoding are what the definitions declare, and a read of such content neither fails nor stores it.
- [ ] The round-trip harness strips the listed exceptions from the expected side, and its self-test still shows that an unexcluded loss fails the comparison.
- [ ] No `io` test pins what ingest does with non-conformant input, and the tests listed above are removed or reworked.
- [ ] The spec's FR-006, FR-016, FR-017 and FR-063, decision 134, and the strictness README are amended as described, and tickets 18 and 26 are updated.
- [ ] The `io` suite and the round-trip suites pass.
