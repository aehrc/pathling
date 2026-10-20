# 25 — State what findResourceDefinition throws for an unknown name

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-007. Old task: T134i.

**What to build:** `DefinitionContext.findResourceDefinition` says nothing about an unknown name, and its implementations differ. `DefaultDefinitionContext` throws `IllegalArgumentException`, while `FhirDefinitionContext` lets HAPI's `DataFormatException` through. `StorableResourceType` therefore catches both by name. Recommended fix: document on the interface that an unknown name throws `IllegalArgumentException`, make `FhirDefinitionContext` translate HAPI's exception, and remove the try/catch and the HAPI import from `StorableResourceType`.

**Blocked by:** None — can start immediately

**Status:** done

- [x] The interface documents the exception, and both implementations throw `IllegalArgumentException` for an unknown name, pinned by tests.
- [x] `StorableResourceType` no longer catches HAPI's exception, and FR-007's unknown-type reporting is unchanged.

## Comments

### Implementation (branch `issue/2830-small-tickets`)

Done as recommended.

- `DefinitionContext.findResourceDefinition` documents `@throws IllegalArgumentException` for a name
  the definitions do not describe as a resource type.
- `FhirDefinitionContext` translates HAPI's `DataFormatException` into that exception, keeping the
  cause.
- `StorableResourceType` calls the lookup without a try/catch or a HAPI import.
- `DefinitionContextAgreementTest.bothImplementationsRefuseAnUnknownNameInTheSameWay` pins both
  implementations. It uses an unknown name, and also a lower-case name, which HAPI would match
  without regard to case.

One thing did change: the message of an unknown-type refusal at ingest. It reads
`Resource type not found: X` where it read `Not a resource type: X`. The exception type is
unchanged, and nothing in the repository, including the Python and R libraries, matches on the
text. `FhirReaderTest.refusesATypeThatMayNotBeStored` passes unchanged.

The other callers of the lookup, as checked:

- Within FHIRPath, a resource name in an expression is checked with `isKnownResourceType` before it
  reaches the lookup. So only a subject resource type supplied by the caller can reach it unknown,
  and there it now raises an `IllegalArgumentException` where it raised HAPI's `DataFormatException`.
- The server's own `DataFormatException` handling, in `ExportOperationValidator`, calls HAPI
  directly, so it is unchanged.
- `ErrorHandlingInterceptor` maps a `DataFormatException` to a 400 but has no mapping for an
  `IllegalArgumentException`, which becomes a 500. If any server route lets an unknown subject type
  reach this lookup, its response would change from a 400 to a 500 once the server takes this
  release. Whether any route does has not been established. It is handed to ticket 26.

- Test runs: `fhir-schema` 41 run, 0 failed; `io` 480 run, 0 failed, 1 skipped; `library-api` 315 run, 0 failed.
