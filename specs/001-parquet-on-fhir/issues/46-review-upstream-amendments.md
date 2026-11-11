# 46 — Review the proposed amendments to the Parquet on FHIR specification, and summarise them for the specification owner

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-001 to FR-005, FR-065. Raised by the re-plan of #2367 (owner, 2026-11-02).

**What to build:** Every change Pathling proposes to the [Parquet on FHIR specification](https://github.com/aehrc/parquet-on-fhir), and every deviation from it, is reviewed and revised by the owner and then summarised in one document. The specification owner discusses that document with the owner before anything more is raised upstream. Under the re-plan, #2367 waits for the amendments to be agreed and aligns Pathling to the outcome (owner, 2026-11-02).

Review each proposal against what is built and what the decisions record, and revise it where they disagree:

- the amendments already raised:
  - [aehrc/parquet-on-fhir#1](https://github.com/aehrc/parquet-on-fhir/issues/1), the canonical annotation's value type;
  - [#2](https://github.com/aehrc/parquet-on-fhir/issues/2), the layout column, from ticket 30;
  - [#3](https://github.com/aehrc/parquet-on-fhir/issues/3), the range annotations' type and `instant`, from ticket 14;
- the amendments drafted in tickets 35 and 44;
- the deviations that Pathling's specification and `contracts/storage-layout.md` record, and any they do not record.

Write the summary to `specs/001-parquet-on-fhir/upstream-amendments.md`. For each change it gives:

- the context, meaning what the specification says today and what Pathling built;
- the issue;
- the proposal and its alternatives;
- other considerations: compatibility, evidence, dependencies, and what Pathling changes if the proposal is rejected.

The summary may also be published as a shared document for the specification owner's comments.

**The draft to start from:** `evidence/upstream-amendments-draft.md` (2026-11-02, revised 2026-11-05 in sections 3, 6, 8 and 16 by the annotations design; committed as evidence by the owner's decision of 2026-11-05). It was `.local/work/issue/2832/upstream-amendments-draft.md`. It lists 17 changes and ends with findings for the owner. Remove those findings before the document is shared. They include:

- **#2 against FR-039.** #2 makes readers refuse a table without the layout column, which FR-039, ADR-0006 and ticket 29 accept. It also names the column `__pof_v1`, where FR-065 says `__layout_v1`. And it cites an evidence file, `evidence/layout-column.md`, that does not exist.
- **#3 against decision 135 and the engine.** #3 names the annotations as siblings, `__birthDate_start`, where decision 135 places them in the metadata group. It also says a reader recomputes a range of the wrong type, which the engine does not do.
- **Decision 138 unmeasured.** Decision 138 rejects `int96` for want of pushdown, which is unmeasured, and which the engine's date comparisons did not get before ticket 43; they now push a plain conjunct on the stored bound (decision 144).
- **#1 and ticket 35 overlap.** Both propose the canonical annotation's type.
- **Deviations not recorded.** `unsignedInt` and `positiveInt` are stored as signed `INT32`, and that is not recorded. Nor is whether `resourceType` is required.

**All 16 findings are worked through in this ticket (owner, 2026-11-05).** The list above names only some of them. Every finding in the draft's closing section, "Cross-cutting findings for the Pathling owner", is either fixed on Pathling's side, raised upstream, or dropped, and the outcome is recorded here. The draft stays committed as it is, findings included, but the findings are left out of anything that goes to the specification owner.

**Do not raise or edit anything on GitHub.** The parent session does that after the owner's review. This applies to revising #1 to #3 as much as to raising new amendments.

**Blocked by:** None. Start early, in parallel with stage 4, because #2367 now waits on the specification owner.

**Status:** ready-for-human

- [ ] Each proposal is reviewed by the owner, and revised where the review asks.
- [ ] `upstream-amendments.md` lists every change and deviation, with the context, issue, proposal and other considerations of each.
- [ ] The summary is discussed with the specification owner, and the outcome for each change is recorded in this ticket's comments.
- [ ] Tickets 30, 35 and 44 are updated to match the outcome before they raise or revise anything upstream.
