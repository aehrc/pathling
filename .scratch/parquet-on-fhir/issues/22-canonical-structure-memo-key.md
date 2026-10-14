# 22 — Key the canonical-structure memo on the resource flag

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-008. Old task: T124e.

**What to build:** `DefinitionCanonicalStructure` caches a structure by type identity alone. The `resource` flag decides whether `resourceType` is added and `contained` dropped, so a cached structure is returned whichever flag the second request carried. This is theoretical today. Either add the flag to the memo key, or assert that one type is never reached both ways.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] A test requests one type both as a resource and not, and gets the correct structure each time, or the assertion fires.
