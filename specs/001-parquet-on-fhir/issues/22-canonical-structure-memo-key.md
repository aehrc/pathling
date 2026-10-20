# 22 — Key the canonical-structure memo on the resource flag

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-008. Old task: T124e.

**What to build:** `DefinitionCanonicalStructure` caches a structure by type identity alone. The `resource` flag decides whether `resourceType` is added and `contained` dropped, so a cached structure is returned whichever flag the second request carried. This is theoretical today. Either add the flag to the memo key, or assert that one type is never reached both ways.

**Blocked by:** None — can start immediately

**Status:** done

- [x] A test requests one type both as a resource and not, and gets the correct structure each time, or the assertion fires.

## Comments

### Implementation (branch `issue/2830-small-tickets`)

The flag is added to the memo key, rather than asserted never to vary, because the fix costs one
line and the assertion would still let the next caller trip over it. The key is built in one place,
`memoKey`, which both the root entry and every later lookup use. That includes the lookup from a
metadata group's extensions. A root stored under one key and found under another would otherwise
build a second structure for a cycle back to it.

- `DefinitionCanonicalStructureTest.keepsAResourceAndTheSameTypeReachedAsAnElementApart` requests
  Patient as a resource and then as an element within one memo. It failed before the fix: the
  element came back with `resourceType`. It now passes, and asking for the resource again returns the
  same instance.
