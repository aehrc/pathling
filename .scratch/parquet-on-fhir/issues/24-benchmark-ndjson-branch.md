# 24 — Remove or restore the benchmark's unreachable NDJSON source

**Spec:** `specs/001-parquet-on-fhir/spec.md` — SC-007. Old task: T124h.

**What to build:** The benchmark's source parameter was narrowed to `delta`, leaving the `ndjson` branch and `initialiseNdjsonSource` unreachable and untested. Either remove them, or restore `ndjson` to the parameter.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] No unreachable source branch remains. The benchmark compiles and a short run succeeds.
