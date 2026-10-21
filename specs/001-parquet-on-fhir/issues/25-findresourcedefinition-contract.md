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
  `IllegalArgumentException`, which becomes a 500. A route that let an unknown subject type reach
  this lookup would therefore answer 500 where it answered 400. The server's routes into the
  FHIRPath engine were checked, and none does, so no mapping was added:
  - Search builds its evaluator from the type of its resource provider. `FhirServer` registers a
    search provider for each of `supportedResourceTypes()`, a list of `ResourceType` values, and one
    for `ViewDefinition`, so every type that reaches a provider is known to the definitions.
  - The patient compartment filter returns no rows for a type with no compartment paths before it
    builds an evaluator, and only known types have paths.
  - A view's resource is parsed with `ResourceType.fromCode` before the engine is called, which
    raises HAPI's `FHIRException` for an unknown name, as it did before.

  The question handed to ticket 26 was therefore settled here and removed from it.
- `FhirResourceResolver.isKnownResourceType` no longer builds a definition to test a name. It asks
  HAPI for the name and compares the canonical name, catching only HAPI's `DataFormatException`,
  which is how HAPI reports a name it does not know. The base branch caught any `Exception`. The
  narrowing is deliberate: any other failure of the lookup, such as a `ConfigurationException` from
  a badly registered custom type, is a fault of the setup, and is now reported rather than turning
  a resource name into a field traversal.
  The test is made for every capitalised identifier at the root of an expression. Building a
  definition there raised and caught three exceptions for each name that is not a resource type, and
  turned a `null` from HAPI into a `NullPointerException`. The canonical-name rule is unchanged.
  `FhirDefinitionContext` now resolves the resource tag only after the name is accepted, so a
  refused name no longer raises a `FHIRException` it does not need.
- Test runs: `fhir-schema` 41 run, 0 failed; `io` 480 run, 0 failed, 1 skipped; `library-api` 315 run, 0 failed.
