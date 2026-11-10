# Parquet on FHIR: remaining work

The tickets that take the [specification](../spec.md) from milestone M4 to its
end state. Each stage is a sub-issue of #2367 and is delivered as one pull
request into `release/11.0.0`, the integration branch for the 11.0.0 release. #2367 is complete when ticket 28 closes. It is not by
itself a release: what the release needs beyond it is raised as follow-up
issues by ticket 26 (owner, 2026-10-22).

**The re-plan (owner, 2026-11-02).** #2367, even split into five stages, was too
big for one issue. Its scope is now a core FHIR library that works, is verified
and well engineered, and matches the functionality of the legacy (Bunsen) line.
It has one extension, primitive metadata (ticket 17). It also reconciles the
layout with the Parquet on FHIR specification upstream: the amendments are
reviewed (ticket 46), agreed with the specification's owner, and Pathling is
aligned to the outcome before #2367 closes.

Everything else, such as dense mode, the XML sink and the shape of SQL output,
moves to GitHub issues after #2367. Each is labelled `PoF_line`, with
`Bunsen_line` where the legacy line has the defect too, and whether it goes into
a release is decided separately. Stage 4 is finished and merged, and stage 5
becomes the final stage.

Each ticket's `Blocked by:` line is the authority on order.

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

JSON and XML are read and written without HAPI, and ingest assumes conformant
input and reports nothing.

- ~~[08](08-spike-ingest-egress-mechanism.md) Spike: the ingest and egress mechanism for JSON and XML~~ (done; choice deferred, decision 134)
- ~~[09](09-decide-column-binding.md) Decide whether column expressions are bound to a schema~~ (done; unbound, decision 136)
- [32](32-xml-and-bundles-without-hapi.md) XML and bundles without HAPI, on the interim ingest
- [37](37-remove-ingest-findings.md) Remove ingest's findings and checks, and list what the layout does not store as exceptions

Decision 134 deferred the choice of ingest mechanism. Until the owner makes it,
JSON stays on Spark's reader and ticket 32 takes XML and bundles off HAPI; ticket
16, in stage 5, is the deferred rework. Ticket 37 removed ingest's findings and
checks once 32 had landed, and ticket 18 then builds the dense mode with no
bounds reporting.

The stage closes early so that stage 4 picks up its changes (owner, 2026-10-23).
Ticket 17 moved to stage 4, which prepares the engine's pairing of `x` with `_x`,
and tickets 16, 29, 30, 35 and 39 moved to stage 5.

## Stage 4: Schema annotations (#2832)

Quantity, decimal and date range annotations are emitted and read, each behind
its own toggle, and their use is decided from the schema. Primitive ids and
extensions are stored and navigable.

- ~~[11](11-annotation-registry-and-toggles.md) Annotation registry, per-annotation toggles and SchemaConfiguration~~ (done)
- ~~[12](12-quantity-annotations.md) Quantity annotations, UCUM-only canonicalisation, codegen-fallback measurement~~ (done)
- ~~[13](13-decimal-annotation.md) Decimal numeric annotation~~ (done)
- ~~[41](41-system-decimal-38-6.md) Switch System.Decimal to DECIMAL(38,6)~~ (done)
- ~~[14](14-date-range-annotation.md) Date range annotation~~ (done)
- ~~[42](42-shared-annotation-computation.md) Memoise an annotation kind's shared computation~~ (done)
- [43](43-lazy-annotation-evaluation.md) Evaluate annotations in the engine only where they are used
- [17](17-placeholder-primitive-metadata-group.md) PLACEHOLDER: the primitive metadata group (US6, #437)
- [15](15-schema-decided-plan-test-and-docs.md) Test that annotation use is decided from the schema, and document annotations
- ~~[34](34-placeholder-content-outside-definitions.md) PLACEHOLDER: Carry content the definitions do not describe through ingest, egress and the engine~~ (moved to a follow-up issue, ticket 26)

The remaining tickets run in the order listed (owner, 2026-11-02). 42 and 43
build the settled annotations design (decisions 140 to 152), which settles how
annotations flow through the engine before anything builds on it. 42 writes the
plan tests for both first, so that any change to what tickets 11 to 14 landed
shows, then memoises the shared computation and makes the quantity's canonical
fields flat; 43 builds the lazy accessors on it (owner, 2026-11-10). 17 is still a placeholder and
is triaged and rewritten before it is dispatched. 15 comes last, so that its
plan tests and documentation describe the final shape.

Ticket 17 moved here from stage 3 (owner, 2026-10-23): it extends ticket 11's
pairing of `x` with `_x` to ids and extensions, and builds on the interim ingest
of ticket 32.

## Stage 5: Verification, reconciliation and completion (#2833)

The amendments to the Parquet on FHIR specification are agreed and Pathling
matches them, performance is measured against the legacy baseline, the engine's
`trust` is documented, the follow-up issues are raised, and #2367 is checked
complete. Re-scoped by the re-plan (owner, 2026-11-02).

Reconciling the specification:

- [46](46-review-upstream-amendments.md) Review the proposed amendments to the Parquet on FHIR specification, and summarise them for the specification owner
- [30](30-pof-spec-layout-column.md) Propose the layout column as an amendment to the Parquet on FHIR specification (raised as aehrc/parquet-on-fhir#2, under review in 46)
- [35](35-pof-spec-json-projection.md) Propose the JSON projection and its prerequisites as amendments to the Parquet on FHIR specification
- [44](44-pof-spec-range-type-int96.md) Reconsider keeping INT96 in the range annotations' amendment
- [45](45-timestamp-type-after-spec.md) Settle the stored timestamp type once the specification is final
- [29](29-layout-version-column.md) Identify the PoF layout by a versioned layout column

Verification and trust:

- [10](10-benchmark-fix-and-extend.md) Benchmark: fix warm planning, add missing views, re-measure
- [27](27-usc-bloodpressures-planning-regression.md) Investigate the UsCoreBloodPressures planning regression
- [31](31-engine-trust-mode.md) The engine runs in `trust`, and conformant data gives correct answers (defects the legacy line also has move to GitHub issues)

Follow-ups and completion:

- [26](26-raise-github-followups.md) Raise the GitHub follow-up issues, including everything the re-plan moved out of #2367
- [36](36-followup-extra-fields.md) Scope the follow-up issue on extra fields: their support, and their place in the conformance levels
- [48](48-annotation-decisions-to-adr.md) Decide which annotation decisions belong in an ADR
- [49](49-dependency-vulnerabilities.md) Clear the dependency vulnerabilities that the pre-release scan reports (after #2879, the move to Spark 4.1)
- [28](28-completion-checklist.md) Completion checklist
- ~~[33](33-engine-layout-tolerance-modes.md) The engine handles non-conformant layout shapes in the same four modes~~ (moved to the follow-up, ticket 26)

Ticket 46 starts early, in parallel with stage 4, because #2367 now waits on the
specification's owner. Tickets 30, 35 and 44 raise or revise nothing upstream
until 46's review. Tickets 29 and 45 follow the agreement on the layout column
and on the range annotations' type. Ticket 31 can start at any time.

## After #2367: GitHub issues

Raised by ticket 26 with the owner's approval, one by one, each linking back to
its ticket (owner, 2026-11-02).

- [18](18-dense-mode.md) Dense schema mode and streaming encode, and [19](19-dense-parity.md) dense parity: `new feature`, `PoF_line`. Until then, a stream is encoded batch by batch.
- [39](39-public-xml-sink-and-source.md) A public XML sink and source: `new feature`, `PoF_line`
- [40](40-sql-output-shape.md) Specify the shape of SQL output, including System types: `enhancement`, `PoF_line`
- [16](16-placeholder-ingest-egress-rework.md) DEFERRED: the ingest mechanism, per-value validation and lossless decimals: `enhancement`, `PoF_line`
- [38](38-annotation-fill-per-row.md) Consider deciding the annotation step's *fill* per row: `enhancement`, `PoF_line`
- Extra fields and their place in the conformance levels, with ticket 36's scope as its text: `enhancement`, `PoF_line`
- Defects the legacy line also has, from ticket 31, such as `toDate()` not checking the calendar: `bug`, `PoF_line`, `Bunsen_line`
- The follow-ups ticket 26 already lists
