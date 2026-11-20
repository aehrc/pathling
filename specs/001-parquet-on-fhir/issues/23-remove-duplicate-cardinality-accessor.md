# 23 — Remove the duplicate cardinality accessor on the default definitions

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-014. Old task: T124f.

**What to build:** `DefaultCompositeDefinition` and `DefaultPrimitiveDefinition` expose `getMaxCardinality()` beside the Lombok `getCardinality()` for the same field. Rename the field so only one accessor remains, and migrate the callers, `YamlSupport` among them.

**Blocked by:** None — can start immediately

**Status:** done

- [x] Each class has one cardinality accessor, and every caller is migrated.
- [x] The `fhir-schema` and `fhirpath` suites pass.

## Comments

### Implementation (branch `issue/2830-small-tickets`)

The field is renamed from `cardinality` to `maxCardinality` in both classes, so the accessor Lombok
derives from it is the `getMaxCardinality()` that `ElementDefinition` declares. The hand-written
getters, and the comments explaining why the old field name was kept, are gone. The parameter of
`DefaultCompositeDefinition.backbone` is renamed with it. `YamlSupport` was the only caller of
`getCardinality()` on these classes. The other `getCardinality()` calls in the repository are on
bitmaps and are unrelated.

- Test runs: `fhir-schema` 41 run, 0 failed; `fhirpath` 7368 run, 0 failed, 922 skipped.
