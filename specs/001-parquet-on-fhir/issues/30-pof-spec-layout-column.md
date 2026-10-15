# 30 — Propose the layout column as an amendment to the Parquet on FHIR specification

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-065. [ADR-0006](../../../docs/adr/0006-layout-identified-by-a-versioned-column.md); decision 132 and its addendum.

**What to build:** A proposal to amend the [Parquet on FHIR specification](https://github.com/aehrc/parquet-on-fhir) so that it requires the layout column, and so other producers write it too. Pathling's FR-065 then takes the column's name and version rule from the specification rather than defining its own.

Read the specification's current text first, and its conventions for fields whose names begin with `__`. The proposal settles:

- **The name.** `__layout_v1` is Pathling's placeholder. Propose a name that follows the specification's naming conventions and identifies the specification, for example `__pof_v1`.
- **The version rule.** Whether `<n>` follows the specification's own version, rising with each version that changes the layout, or is a separate layout counter. Tying it to the specification's version is the simpler option, unless the specification's versioning makes that awkward.
- **Type and value.** A nullable boolean whose every value is null. Recognised by name and type alone, so a reader needs only the schema, and an empty table is identified as well as a full one. Give the reasons from ADR-0006, including why the version is not in the value.
- **Status.** Required of every conforming producer, and not an annotation. The specification's annotations are optional and derived; this column is neither, so it needs its own definition, so that implementers do not treat it as optional.
- **Reader behaviour.** A reader refuses a version higher than it supports, and how it treats a dataset with no layout column.

**Do not raise anything on GitHub.** Write the proposal, as the title and body of an issue or the text of a pull request on `aehrc/parquet-on-fhir`, to `.local/work/<branch>/pof-spec-amendment.md`. The owner reviews it, and the parent session raises it. Record the number here once it is raised.

**Blocked by:** None

**Status:** ready-for-agent

- [ ] The proposal is drafted, covering the name, the version rule, type and value, status and reader behaviour, with the reasons for each.
- [ ] It is raised on `aehrc/parquet-on-fhir` after the owner's review, and its number is recorded in this ticket's comments.
- [ ] Once it is agreed, FR-065, ADR-0006, the data model and ticket 29 use the agreed name and version rule, or a follow-up records the rename if ticket 29 has already landed.
