# 26 — Raise the GitHub follow-up issues

**Spec:** `specs/001-parquet-on-fhir/spec.md`. Old tasks: T134c, T134d, T134e (removed by ticket 37).

**What to build:** Follow-ups that outlive this programme are recorded as GitHub issues, each with the context from its frozen task or decision.

- **T134c.** The object-storage half of the T119 merge gate. The merge cost on S3 for raw files is unmeasured, so R-015's fallback stays open.
- **T134d.** Whether the round-trip harness takes the `test-data` bulk corpus: a 124 MB test-scope download into the core chain, and how much to read per type. It widens the resource-type spread, not a substitute for the specification examples.
- **Dense-shaped output for FHIR-typed results.**
- **The #2804 renderer note.** Add a comment to #2804 recording what ticket 03 left for System.Coding in the renderer.
- **The range of a time without a timezone.** Such a value is valid (decision 133's addendum), and ticket 14 gives it the range the engine computes today. Whether that range, in the engine and in the annotation, should widen by ±14 hours because the value could be in any zone (`evidence/ingress-strictness/README.md`).
- **Non-conformant data beyond `trust` (owner, 2026-10-22).** #2367 delivers `trust` only, at ingest and in the engine. The issue covers the rest, from `evidence/ingress-strictness/README.md` ("Conformance levels", "Modes", "Exceptions to the levels") and decision 133's addendum of 2026-10-22:
  - the modes `strict`, `tolerate` and `repair`, each with findings or silent, and the default for ingest and for the engine;
  - a `validate` operation that collects every violation as findings, over a source or stored data;
  - ingest's presets over decision 133's repair levels and policies, and ticket 16's per-value validation;
  - the decimal grammar in `fhir-schema`, beside ticket 31's date grammar, and its use in decimal decoding and at ingest;
  - the engine's handling of invalid values and layout shapes, from ticket 31's former design (`issues/31-engine-value-tolerance-modes.md` at `cbce542dd5`, and "Modes" in `evidence/ingress-strictness/README.md`) and ticket 33;
  - a schema that does not conform, level 3 in the engine, and whether `trust` checks it;
  - whether the engine uses an annotation stored by another producer or recomputes it, which decision 135 assigns to the modes rather than to ticket 11's *recompute* setting;
  - the exceptions still open: `null` and `[]` in JSON, the empty string's level, and whether trimming whitespace is lossless;
  - FR-063, rewritten to match the softened principles 2 and 5;
  - re-enabling the tests excluded for the non-conformant fixture `fhirpath-js/resources/quantity-example.json`, whose `QuestionnaireResponse.item.answer` is an object where FHIR JSON requires an array. Under `trust` ingest fails on it with Spark's own error (ticket 37), so stage 3 excludes it, and the tests read it again once ingest tolerates such input (owner, 2026-10-23). The exclusions are the rule "Non-conformant input fixture: answer is not an array" in `fhirpath/src/test/resources/fhirpath-js/config.yaml`, `NON_CONFORMANT_FIXTURES` in `AnnotationFreeSuiteTest`, and the fixture's omission from `RESOURCE_FILES` in `FhirPathToColumnTest`.
- **Extra fields (owner, 2026-10-22).** Content and layout that the definitions do not derive: their support through ingest, egress and the engine, and their place in the conformance levels. Ticket 34, which this issue replaces, and the metadata that would preserve such content's representation. The issue's scope is written up in ticket 36.
- **The server without Woodstox (owner, 2026-10-22).** Ticket 32 excluded `woodstox-core` from `encoders`, and so from `library-api` and `library-runtime`, because it made Spark's `to_xml` fail. When the server moves to this runtime, check that its HAPI XML responses work without Woodstox from `library-runtime`, and whether its own dependencies still bring Woodstox in.
- **What the release needs beyond #2367 (owner, 2026-10-22).** #2367 is not by itself a release. Each follow-up the release needs that arises from this work is raised as an issue, including the release notes that ticket 28 hands on.
- **Annotation kinds as plugins (owner, 2026-10-23).** Decision 135's addendum of 2026-10-23 keeps the set of kinds closed. The issue covers an extensible kind registry; the engine reporting the kinds it uses, so that the annotation step can be configured from it; and the identity of a kind: name collisions between kinds, versioned computations, such as `__canonical`'s dependence on the UCUM table, and data that outlives the engine that wrote it.
- **Profiling and optimising query performance (owner, 2026-10-26).** What the engine loses when a plan falls out of whole-stage code generation, and how that interacts with the JIT compiler declining to compile large generated methods (`-XX:-DontCompileHugeMethods`, 8,000 bytecodes). The issue profiles and optimises every candidate, not one operator at a time. Candidates known so far:
  - the quantity `=` and `!=`, which fall back through `ArrayFilter`, `ArrayExists` and `ArrayForAll` (decision 103, and ticket 12's T096c measurement in `evidence/ticket-12-quantity-annotations.md`);
  - every view's `Generate`, and `UsCoreBloodPressures`'s Coding `Filter` with its 336 higher-order lambdas (decision 126);
  - the size of the generated methods, measured by T100o (decision 102).

  Ticket 12 rewrote nothing, because no measured cost showed. The issue asks the engine-wide question that decision 126 left open.
- **Others handed on.** Any follow-up passed on by ticket 10, such as a precomputed reference key. Ticket 08's threads, such as the upstream Spark issue from T132, moved to ticket 16, which raises its own.

**Blocked by:** None — can start immediately. Hand-offs from 10 are added as they arrive.

**Status:** ready-for-human

- [ ] Each issue is raised, and its number is recorded in this ticket's comments.
- [ ] Each issue carries enough context that nobody has to rediscover it from `tasks.md`.
