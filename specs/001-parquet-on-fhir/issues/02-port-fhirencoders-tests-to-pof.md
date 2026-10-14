# 02 — Port the engine tests that encode via FhirEncoders to the PoF layout

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-046, SC-002.

**What to build:** Several engine tests still build their input with the legacy `FhirEncoders`, so they exercise the legacy-layout branches that ticket 05 deletes. Port them to build their data in the PoF layout through the public read and encode path, keeping each test's intent:

- `AbsentElementTest`
- `ProjectedColumnTypeTest`
- `SiblingCombinationTest`
- `DivergentSchemaViewTest`
- `YamlTestRunnerTest.testJsonModel`

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] No test in the list encodes through `FhirEncoders` or reads legacy-layout data.
- [ ] Each ported test asserts what it asserted before. Where an assertion depended on the legacy shape, such as a fixed-scale decimal or `_fid`, it is restated against the PoF shape, and the change is called out for review.
- [ ] A search confirms that no other test under `fhirpath` encodes via `FhirEncoders`, or lists those that do as further work on this ticket.
- [ ] The `fhirpath` suite passes.
