# 23 — Remove the duplicate cardinality accessor on the default definitions

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-014. Old task: T124f.

**What to build:** `DefaultCompositeDefinition` and `DefaultPrimitiveDefinition` expose `getMaxCardinality()` beside the Lombok `getCardinality()` for the same field. Rename the field so only one accessor remains, and migrate the callers, `YamlSupport` among them.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] Each class has one cardinality accessor, and every caller is migrated.
- [ ] The `fhir-schema` and `fhirpath` suites pass.
