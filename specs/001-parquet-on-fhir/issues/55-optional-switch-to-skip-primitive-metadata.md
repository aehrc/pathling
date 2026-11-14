# 55 — Optional switch to skip reading primitive metadata

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-034, US6. Raised from ticket 54 (Primitive metadata always on; owner, 2026-11-13). Decisions 141 (D14) and 154.

**What to build:** Triage whether the engine needs an option to leave a primitive's `id` and `extension` out of its elements, which ticket 54 removed (owner, 2026-11-13, decision 154: primitive metadata is always on).

The cost that the switch bounded, and that this ticket weighs:

- Once a primitive without a value but with an id or extensions is an element, the test of whether it is an element, `_x.extension IS NOT NULL`, reads the whole Extension struct of every primitive the expression reads, singular or repeating. The owner accepted that read (ticket 52, review finding 3). The candidate fix, testing `extension.url`, reads about 17.6 MB rather than 182.4 MB on 1M Patients with extension-heavy `_birthDate` (`evidence/annotations-design-singular-meta-pruning.md`), and is in ticket 26.
- The design's constraint that an expression reads only the metadata it uses (`design/annotations-in-the-engine.md` §2) was a target for the final solution, not a release gate (owner, 2026-11-04). With one mode, §4.1 no longer meets it for ids and extensions.

Questions for triage: whether the measured cost justifies a switch once the `extension.url` test is in; where it would live (not `QueryConfiguration`, which the server binds); and whether it should be an option or an analysis of the expression (principle P0, decision 140, rules out the latter).

**Blocked by:** 54.

**Status:** needs-triage
