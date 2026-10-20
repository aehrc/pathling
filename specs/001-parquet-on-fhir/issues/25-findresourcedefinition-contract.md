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

The server is not affected. Its `DataFormatException` handling, in `ExportOperationValidator`, calls
HAPI directly, not through `DefinitionContext`.

- Test runs: `fhir-schema` 41 run, 0 failed; `io` 480 run, 0 failed, 1 skipped.
