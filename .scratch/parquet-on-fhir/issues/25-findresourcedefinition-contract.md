# 25 — State what findResourceDefinition throws for an unknown name

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-007. Old task: T134i.

**What to build:** `DefinitionContext.findResourceDefinition` says nothing about an unknown name, and its implementations differ. `DefaultDefinitionContext` throws `IllegalArgumentException`, while `FhirDefinitionContext` lets HAPI's `DataFormatException` through. `StorableResourceType` therefore catches both by name. Recommended fix: document on the interface that an unknown name throws `IllegalArgumentException`, make `FhirDefinitionContext` translate HAPI's exception, and remove the try/catch and the HAPI import from `StorableResourceType`.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] The interface documents the exception, and both implementations throw `IllegalArgumentException` for an unknown name, pinned by tests.
- [ ] `StorableResourceType` no longer catches HAPI's exception, and FR-007's unknown-type reporting is unchanged.
