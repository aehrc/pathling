# 26 — Raise the GitHub follow-up issues

**Spec:** `specs/001-parquet-on-fhir/spec.md`. Old tasks: T134c, T134d, T134e.

**What to build:** Follow-ups that outlive this programme are recorded as GitHub issues, each with the context from its frozen task or decision.

- **T134c.** The object-storage half of the T119 merge gate. The merge cost on S3 for raw files is unmeasured, so R-015's fallback stays open.
- **T134d.** Whether the round-trip harness takes the `test-data` bulk corpus: a 124 MB test-scope download into the core chain, and how much to read per type. It widens the resource-type spread, not a substitute for the specification examples.
- **T134e.** Mis-diagnosed findings. An inline resource declared as abstract `Resource` (`Parameters.parameter.resource`, `Bundle.entry.resource`) is reported as undescribed content. The likely remedy is a fourth `NonConformantContent` kind. An empty array is reported as "a value where a structure is declared".
- **Dense-shaped output for FHIR-typed results.**
- **The #2804 renderer note.** Add a comment to #2804 recording what ticket 03 left for System.Coding in the renderer.
- **Server response to an unknown subject resource type (from ticket 25).** `FhirDefinitionContext` now raises `IllegalArgumentException` for an unknown name, where it let HAPI's `DataFormatException` through. The server's `ErrorHandlingInterceptor` maps the latter to a 400 and the former to a 500. Check whether any server route lets a user-supplied resource type reach the lookup unvalidated. If one does, validate the type at that route or map the exception.
- **Others handed on.** Any follow-up passed on by tickets 08 and 10 (for example the upstream Spark issue from T132, or a precomputed reference key).

**Blocked by:** None — can start immediately. Hand-offs from 08 and 10 are added as they arrive.

**Status:** ready-for-human

- [ ] Each issue is raised, and its number is recorded in this ticket's comments.
- [ ] Each issue carries enough context that nobody has to rediscover it from `tasks.md`.
