# Parquet on FHIR: remaining work

The tickets that take the [specification](../spec.md) from milestone M4 to its
end state, grouped into five stages. Each stage is a sub-issue of #2367 and is
delivered as one pull request into `issue/2367`. #2367 is complete when ticket 28
closes. It is not by itself a release: what the release needs beyond it is raised
as follow-up issues by ticket 26 (owner, 2026-10-22).

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

## Stage 2: Harden (#2830)

The known correctness gaps are closed. The benchmark moved to stage 5, so that it
measures the finished layout, annotations included.

- [01](01-keep-file-counter-on-rename.md) Keep the file counter so split partition files do not collide on rename
- ~~[20](20-egress-ignores-unknown-field.md) Egress ignores and reports a stored field it does not know~~ (folded into 34)
- [21](21-pin-type-over-choice.md) Pin FHIRPath type() over a choice element
- [22](22-canonical-structure-memo-key.md) Key the canonical-structure memo on the resource flag
- [23](23-remove-duplicate-cardinality-accessor.md) Remove the duplicate cardinality accessor on the default definitions
- [25](25-findresourcedefinition-contract.md) State what findResourceDefinition throws for an unknown name
- [24](24-benchmark-ndjson-branch.md) Remove or restore the benchmark's unreachable NDJSON source

## Stage 3: Ingest and egress without HAPI (#2831)

JSON and XML are read and written without HAPI, and primitive ids and
extensions are stored.

- ~~[08](08-spike-ingest-egress-mechanism.md) Spike: the ingest and egress mechanism for JSON and XML~~ (done; choice deferred, decision 134)
- [09](09-decide-column-binding.md) Decide whether column expressions are bound to a schema
- [32](32-xml-and-bundles-without-hapi.md) XML and bundles without HAPI, on the interim ingest
- [16](16-placeholder-ingest-egress-rework.md) DEFERRED: the ingest mechanism, per-value validation and lossless decimals
- [17](17-placeholder-primitive-metadata-group.md) PLACEHOLDER: the primitive metadata group (US6, #437)
- [29](29-layout-version-column.md) Identify the PoF layout by a versioned layout column
- [30](30-pof-spec-layout-column.md) Propose the layout column as an amendment to the Parquet on FHIR specification
- [35](35-pof-spec-json-projection.md) Propose the JSON projection and its prerequisites as amendments to the Parquet on FHIR specification

Decision 134 deferred the choice of ingest mechanism. Until the owner makes it,
JSON stays on Spark's reader and ticket 32 takes XML and bundles off HAPI; ticket
16 is the deferred rework. Tickets 17 and 18 build on the interim ingest, so they
follow 32. Ticket 29 adds the layout column to the ingest #2367
delivers, so it follows 32. Until it
lands, the refusal of the legacy layout rests on the root `id_versioned`. Ticket 30
can start at any time; 29 takes its name from 30 if 30 is agreed first. Ticket 35 can
also start at any time, and #2367 does not wait for its amendments to be agreed.

## Stage 4: Schema annotations (#2832)

Quantity, decimal and date range annotations are emitted and read, each behind
its own toggle, and their use is decided from the schema.

- [11](11-annotation-registry-and-toggles.md) Annotation registry, per-annotation toggles and SchemaConfiguration
- [12](12-quantity-annotations.md) Quantity annotations, UCUM-only canonicalisation, codegen-fallback measurement
- [13](13-decimal-annotation.md) Decimal numeric annotation
- [14](14-date-range-annotation.md) Date range annotation
- [15](15-schema-decided-plan-test-and-docs.md) Test that annotation use is decided from the schema, and document annotations
- ~~[34](34-placeholder-content-outside-definitions.md) PLACEHOLDER: Carry content the definitions do not describe through ingest, egress and the engine~~ (moved to a follow-up issue, ticket 26)

Content the definitions do not describe, ticket 34 until 2026-10-22, is outside
the round-trip invariant and moves to a follow-up issue that ticket 26 raises.
Ticket 36 writes that issue's scope, which now covers extra fields generally,
including `__` fields from other producers, and their place in the
conformance levels.

## Stage 5: Dense mode, measurement, trust and completion (#2833)

The dense schema mode matches the fitted one, the benchmark is re-measured
against the finished layout, the engine's `trust` is documented, the follow-up
issues are raised, and #2367 is checked complete.

- [18](18-dense-mode.md) Dense schema mode, with bounds reporting and streaming encode
- [19](19-dense-parity.md) Dense parity
- [10](10-benchmark-fix-and-extend.md) Benchmark: fix warm planning, add missing views, re-measure
- [27](27-usc-bloodpressures-planning-regression.md) Investigate the UsCoreBloodPressures planning regression
- [31](31-engine-trust-mode.md) The engine runs in `trust`, and conformant data gives correct answers
- ~~[33](33-engine-layout-tolerance-modes.md) The engine handles non-conformant layout shapes in the same four modes~~ (moved to the follow-up, ticket 26)
- [26](26-raise-github-followups.md) Raise the GitHub follow-up issues, including non-conformant data beyond `trust`, extra fields, and what the release needs
- [36](36-followup-extra-fields.md) Scope the follow-up issue on extra fields: their support, and their place in the conformance levels
- [28](28-completion-checklist.md) Completion checklist

Ticket 31 can start at any time, except its decimal criterion,
which waits for ticket 13. Handling non-conformant data beyond `trust`,
which 31 and 33 held until 2026-10-22, is the follow-up issue that ticket 26
raises.
