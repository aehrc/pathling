# Parquet on FHIR: remaining work

The tickets that take the [specification](../spec.md) from milestone M4 to its
end state, grouped into five stages. Each stage is a sub-issue of #2367 and is
delivered as one pull request into `issue/2367`. Everything ships together, in
one release, when ticket 28 closes.

Each ticket's `Blocked by:` line is the authority on order. The stages group
tickets so that each one leaves the integration branch in a coherent state.
They are not a strict sequence. Ticket 08, the spike, gates stages 3 and 5, so
it starts during stage 1. Ticket 11 can likewise be settled early, and 26
collects follow-ups throughout.

## Stage 1: Retire the old layout (#2829)

Only the new layout is read or written. Data in the old layout has a documented
way out.

- [02](02-port-fhirencoders-tests-to-pof.md) Port the engine tests that encode via FhirEncoders to the PoF layout
- [03](03-system-quantity-fixed-shape.md) Engine-built System.Quantity takes one fixed shape, not the legacy one
- ~~[04](04-loud-failure-legacy-column.md) Fail loudly on a legacy-typed column at traversal~~ (dropped, decision 132)
- [05](05-remove-legacy-normalisation.md) Remove the legacy-layout normalisation, its test dimension and CI job
- [06](06-legacy-export.md) Legacy export: legacy Parquet/Delta to FHIR NDJSON, as a component and CLI
- [07](07-legacy-export-docs-and-server-followup.md) Document the legacy export; raise the server migration issue

## Stage 2: Harden and measure (#2830)

The known correctness gaps are closed and the benchmark can be trusted as the
baseline for the reworks that follow.

- [01](01-keep-file-counter-on-rename.md) Keep the file counter so split partition files do not collide on rename
- [20](20-egress-ignores-unknown-field.md) Egress ignores and reports a stored field it does not know
- [21](21-pin-type-over-choice.md) Pin FHIRPath type() over a choice element
- [22](22-canonical-structure-memo-key.md) Key the canonical-structure memo on the resource flag
- [23](23-remove-duplicate-cardinality-accessor.md) Remove the duplicate cardinality accessor on the default definitions
- [25](25-findresourcedefinition-contract.md) State what findResourceDefinition throws for an unknown name
- [10](10-benchmark-fix-and-extend.md) Benchmark: fix warm planning, add missing views, re-measure
- [24](24-benchmark-ndjson-branch.md) Remove or restore the benchmark's unreachable NDJSON source
- [27](27-usc-bloodpressures-planning-regression.md) Investigate the UsCoreBloodPressures planning regression

## Stage 3: Ingest and egress without HAPI (#2831)

JSON and XML are read and written without HAPI, and primitive ids and
extensions are stored.

- [08](08-spike-ingest-egress-mechanism.md) Spike: the ingest and egress mechanism for JSON and XML
- [09](09-decide-column-binding.md) Decide whether column expressions are bound to a schema
- [16](16-placeholder-ingest-egress-rework.md) PLACEHOLDER: ingest and egress rework per the spike
- [17](17-placeholder-primitive-metadata-group.md) PLACEHOLDER: the primitive metadata group (US6, #437)
- [29](29-layout-version-column.md) Identify the PoF layout by a versioned layout column
- [30](30-pof-spec-layout-column.md) Propose the layout column as an amendment to the Parquet on FHIR specification

Tickets 16 and 17 are rewritten from the spike's findings before they are built.
Ticket 29 adds the layout column to the rebuilt ingest, so it follows 16. Until it
lands, the refusal of the legacy layout rests on the root `id_versioned`. Ticket 30
can start at any time; 29 takes its name from 30 if 30 is agreed first.

## Stage 4: Schema annotations (#2832)

Quantity, decimal and date range annotations are emitted and read, each behind
its own toggle, and their use is decided from the schema.

- [11](11-annotation-registry-and-toggles.md) Annotation registry, per-annotation toggles and SchemaConfiguration
- [12](12-quantity-annotations.md) Quantity annotations, UCUM-only canonicalisation, codegen-fallback measurement
- [13](13-decimal-annotation.md) Decimal numeric annotation
- [14](14-date-range-annotation.md) Date range annotation
- [15](15-schema-decided-plan-test-and-docs.md) Test that annotation use is decided from the schema, and document annotations

## Stage 5: Dense mode and release (#2833)

The dense schema mode matches the fitted one, and the release is checked.

- [18](18-dense-mode.md) Dense schema mode, with bounds reporting and streaming encode
- [19](19-dense-parity.md) Dense parity
- [26](26-raise-github-followups.md) Raise the GitHub follow-up issues
- [28](28-release-checklist.md) Release checklist
