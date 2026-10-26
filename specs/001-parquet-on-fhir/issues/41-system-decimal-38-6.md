# 41 — Switch System.Decimal to DECIMAL(38,6)

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-002. The Parquet on FHIR specification's `numeric` annotation. Raised from ticket 13 (owner, 2026-10-26).

**What to build:** The engine's System.Decimal becomes `DECIMAL(38,6)`, the type the [Parquet on FHIR specification](https://github.com/aehrc/parquet-on-fhir) gives the `numeric` annotation. Ticket 13 gives `__numeric` System.Decimal's type, so after this ticket the stored annotation matches the specification, and the engine reads it without a conversion.

**Why it changes few results.** Spark computes `+`, `-`, `*` and `/` over `DECIMAL(32,6)` operands at `DECIMAL(38,6)` already, and the engine casts the result back to System.Decimal. Both types keep 6 fractional digits, so rounding is unchanged. What changes is the range: a value with 27 to 32 integer digits, null today, now has a value. For example, `1e14 * 1e14` gives `1e28` rather than null, and a stored `1e30` decodes rather than becoming null.

**What it touches.**

- **One definition.** System.Decimal's type is defined once, in the module ticket 13 put it in, and no longer borrows its precision from `DecimalCustomCoder`. `DecimalCustomCoder`, the legacy layout's encoder, stays `DECIMAL(32,6)`, because `legacy-export` and the layout detection read legacy data at that type.
- **Its users.** `DecimalCollection` (decoding, arithmetic, literals and the precision check in `toLiteral`), `QuantityEncoding` (a quantity's value), `ConversionLogic` (`toDecimal()`), `IntegerCollection` (integer to decimal), `FlexiDecimal.fromValue` and `terminology`'s `PropertyUdf` take System.Decimal's type rather than `DecimalCustomCoder`'s.
- **Output.** A decimal column in a view's output, in Java, Python and R, becomes `DECIMAL(38,6)`. That is a change users see, so it goes in the release notes, and ticket 40 records it in the shape of SQL output.
- **Comments and docs** that name `DECIMAL(32,6)` as the engine's type.

**An owner-approval gate.** Each existing test whose expected value changes is listed with its old and new values before it is changed. Known cases: `AnsiModeIndependenceTest` (`1e28`), `ColumnRepresentationSafeCastTest` (`1e30`) and `ProjectedColumnTypeTest` (the output type).

**A recorded deviation (owner, 2026-10-26).** FHIRPath requires a Decimal to provide at least the range (-10^28+1)/10^8 to (10^28-1)/10^8 with a step of 10^-8, so 8 fractional digits (`DECIMAL(28,8)`). `DECIMAL(38,6)` covers the range but keeps 6 fractional digits, as `DECIMAL(32,6)` does today. Record this as a known deviation in the specification. Whether to move to `DECIMAL(38,8)`, with an upstream proposal for the annotation's type, is a follow-up issue that ticket 26 raises.

**Blocked by:** 13

**Status:** ready-for-agent

- [ ] System.Decimal is `DECIMAL(38,6)`, defined once, and `DecimalCustomCoder` stays `DECIMAL(32,6)`.
- [ ] Every engine use of a decimal type takes System.Decimal's, and `__numeric` is stored at `DECIMAL(38,6)`.
- [ ] Each changed test expectation is approved by the owner before it is changed.
- [ ] The output type change is documented, and the 8-digit deviation is recorded in the specification.
