# 40 — Specify the shape of SQL output

> **Moved after #2367 (owner, 2026-11-02).** This ticket becomes a GitHub issue raised by ticket 26, and is not dispatched within #2367. See this ticket's comments.

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-021, FR-059; `contracts/engine-semantics.md`; decision 135. Raised from ticket 12's first review item (owner, 2026-10-26).

**What to decide:** the shape of what the engine hands back as SQL, where a result leaves FHIRPath for Spark columns: `fhirPathToColumn`, and view columns where they allow a complex type.

Today the shape follows whatever the plan built. Ticket 12 kept the canonical annotations on a returned quantity for now (owner, 2026-10-26), as 9.9.0 kept `_fid`, `value_scale` and the canonical companions there. `evaluateFhirPath` strips annotations, but only when it renders JSON. Nothing specifies either behaviour.

The specification should cover at least:

- **Pruning annotations.** Whether `__` fields leave the engine, and if they do, whether the FR-021 switches govern them.
- **Fixed types for primitives.** One Spark type per FHIR primitive in output, whatever the stored or computed representation. For example, a decimal is returned as one type, not as the value of one plan and the FlexiDecimal of another. Ticket 41 makes that type `DECIMAL(38,6)`, and gives the System.Quantity `_value.__numeric`, which the output shape records.
- **A unified, complete schema for complex types.** A complex result could have one schema derived from the definitions, rather than the fitted shape of the dataset it was read from. Then the same expression returns the same type over any dataset.

Out of scope: JSON and XML egress, whose shape is FHIR's.

**Blocked by:** None. It is best taken once tickets 12–14 have settled which annotations exist.

**Status:** needs-triage

- [ ] The owner agrees the output shape for annotations, primitives and complex types, and it is recorded in the specification.
- [ ] Tickets are raised for whatever the agreed shape requires the engine to change, or for the follow-up issues through ticket 26.

## Comments

### For review: a returned System.Quantity drops its canonical annotations (owner, 2026-11-10)

Under decisions 150 and 152 a System.Quantity's canonical annotations, `__canonical_code` and
`__canonical_value_exact`, are present only where available without computing. Kept on output, they
would make the returned shape depend on the data. The owner chose to drop them: a System.Quantity
returned as a column is `{value, unit, code, system}`. This reverses the 2026-10-26 decision recorded
above, which kept them, and the alternative, filling them at output so the shape stays fixed, is to
be reconsidered here with the rest of the output shape. Recorded as an amendment to decision 150;
`contracts/engine-semantics.md` and `contracts/library-api.md` describe the dropped shape.

### Moved after #2367 (owner, 2026-11-02)

The re-plan limits #2367 to a core library that matches the legacy line, with primitive metadata as its one extension. This ticket becomes a GitHub issue, raised by ticket 26 and labelled `enhancement` and `PoF_line`, and whether it goes into a release is decided separately.

### Ticket 47 folded in: what `fhirPathToColumn` and `$fhirpath` return (owner, 2026-11-05)

Ticket 47 was raised on 2026-11-05 to specify these return values, decimals first, and was folded into this ticket the same day, because this ticket already covers `fhirPathToColumn` and the fixed types of primitives. Ticket 47 is retired, and its number is not reused.

A decimal leaves the engine differently on each path today:

- **A view column** is a string: the `DECIMAL(38,6)` value as plain text, with trailing zeros trimmed (`DecimalCollection.toExternalValue`, `DecimalToLiteral`), so a stored `1.50` is output as `1.5`. It was a string before 10.0.0 was merged into the line; the merge changed only the type the column reports (`efe1dfa4fa`).
- **`fhirPathToColumn`** returns the engine's numeric, `DECIMAL(38,6)`, through `getOutputColumn()` (`SearchColumnBuilder`): a stored `1.50` gives `1.500000`.
- **`$fhirpath`** single-instance evaluation selects the same column and collects a decimal as a `BigDecimal` (read from the code, not run).

The owner decided (2026-11-05) that `fhirPathToColumn` and `$fhirpath` stay numeric for now, whatever ticket 16 brings. The stored-decimal text reaches only views and `toString()`. This ticket therefore also specifies, on purpose rather than by accident:

- **Decimals on these two paths.** Whether a stored decimal returns its stored text, the numeric, or both, and the numeric's type and scale.
- **Other primitives and System types.** One type per result type on each path, consistent with the output shape above.
- **`$fhirpath`'s rendered values,** where they differ from the column.

Any change to a public return type is flagged for the release notes and semver. Taken after ticket 16 if stored text is chosen.
