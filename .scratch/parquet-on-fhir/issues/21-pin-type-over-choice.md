# 21 — Pin FHIRPath type() over a choice element

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-013. Old task: T124d.

**What to build:** Engine tests pinning `type()` over a choice element, including one that can hold a reference, such as `Extension.value`. M1 changed `FhirChoiceDefinition.getAllChildTypes` to declared order with one reference variant. That method also feeds `TypeInfo.choiceTypeInfoMapper`, so engine behaviour moved with no test to show it.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] Tests cover `type()` over a non-reference choice and a reference-capable choice.
- [ ] The same expectations are checked against the pre-M1 definition code, for example the `main` tag before the programme. Any disagreement is reported to the owner.
