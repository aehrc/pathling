# 35 — Propose the JSON projection and its prerequisites as amendments to the Parquet on FHIR specification

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-019, FR-065. "Conformance levels" and "The round-trip invariant" in `evidence/ingress-strictness/README.md`. Ticket 30 is the layout column's amendment, to the same specification.

**What to build:** Proposals that make the [Parquet on FHIR specification](https://github.com/aehrc/parquet-on-fhir) define how a PoF row maps to FHIR JSON, so that level 2 conformance for PoF is defined by reference to [FHIR's JSON rules](https://hl7.org/fhir/R4/json.html) rather than by a list of its own, and so that the round-trip invariant can be stated: for FHIR that conforms to a given definition set, JSON → PoF → JSON is an identity up to JSON equivalence, with the gaps the evidence lists. Content the definitions do not describe is outside it (owner, 2026-10-22).

Each amendment is raised as its own issue on `aehrc/parquet-on-fhir`, so that each can be agreed on its own:

1. **The projection, level 2 and the invariant.** A null field in a structure is omitted, and a null in an array is written as `null`. A PoF dataset conforms at level 2 when its projection conforms to FHIR JSON. That excludes `[]`, a structure whose every field is null, a null item in an array of structures, `""`, and a positional null with no aligned `_x`, without the specification restating them. A value's JSON type, such as a decimal stored as text written as a number, comes from the definitions.
2. **The `__` prefix is reserved.** A field whose name starts with `__` is layout, such as an annotation or the layout column, and the projection leaves it out. Check how far the specification's `__<field>_canonical` naming already says this.
3. **`resourceType` at the root**, in every row, so that the row carries everything the projection writes.
4. **Annotations live in the container their element owns (decision 135).** A primitive's annotations become fields of its `_x` group, after `id` and `extension` (`_birthDate.__start`), and a complex element's become fields of its own structure (`valueQuantity.__canonical`), replacing the `__<element>_<annotation>` siblings. The reasons: a repeating primitive's annotations share the alignment FHIR already gives `_x`, which the specification does not address for repeating annotated elements today, and a quantity's canonical form travels with the value. The projection omits an `_x` group left with only `__` fields. Correct the Quantity example with it: its `__valueQuantity_canonical` group copies the Quantity shape, which contradicts the section's table of `value` and `code`.

**Do not raise anything on GitHub without the owner's review.** Read the specification's current text first. Write each proposal, as an issue's title and body, to `.local/work/<branch>/pof-spec-amendment-<n>.md`. The owner reviews them, and the parent session raises them. Record each number here once it is raised.

**Not here.**

- A marker in the Parquet metadata giving a column's JSON type, and anything else that preserves the representation of content the definitions do not describe. That is the follow-up issue ticket 26 raises in place of ticket 34.
- Pathling's own changes once the amendments are agreed, which become tickets or follow-up issues of their own: egress as the projection, the `resourceType` column, and FR-019's pruning, which a conformant dataset no longer needs.

**Blocked by:** None. #2367 does not wait for agreement upstream.

**Status:** needs-triage

- [ ] A proposal is drafted for each amendment above, with the reasons for each.
- [ ] Each is raised on `aehrc/parquet-on-fhir` after the owner's review, and its number is recorded in this ticket's comments.
- [ ] Once they are agreed, the Pathling changes are ticketed or raised as follow-up issues.
