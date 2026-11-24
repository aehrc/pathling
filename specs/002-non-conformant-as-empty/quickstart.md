# Quickstart: validating the feature end to end

Each scenario uses the shared corpus in `io/src/test/resources/nonconformant/` and runs under both
`spark.sql.ansi.enabled` settings.

1. **Conformant data is unchanged.** Run `mvn install -pl library-runtime -am`, then the Python and R suites.
   Every suite passes, and the timezone-less `dateTime` and `instant` tests pass (US1).
2. **A messy extract loads.** Encode `nonconformant/patients.ndjson`, which holds every level 2 and level 3
   shape, and write it to Delta. The write succeeds. `Patient.name.count()`, `Patient.birthDate` and
   `Patient.multipleBirthInteger` give the answers in `data-model.md` (US2).
3. **JSON and XML agree.** Encode `nonconformant/patients.xml`, the same content in XML. The stored data
   equals that of scenario 2 (SC-004).
4. **Arrays keep their valid items.** On scenario 2's data, `name.given.count()` and `name.given.first()`
   skip invalid and unpaired items, and keep a null with an extension in place (US3).
5. **Foreign Parquet does not fail.** Write the corpus's directly written PoF cases (an invalid date, a
   decimal `abc`, `[null, "Ann"]` with no metadata) with and without annotations. Every comparison,
   arithmetic and conversion query succeeds, and the two variants give identical answers (US4, SC-003).
6. **Documentation.** `site/docs/libraries/io/schema.md` states the rule, the exceptions and the divergences,
   with one example per row of `data-model.md` (FR-019, FR-020).
7. **After OQ-A:** `maritalStatus: {}` and `name: [{"family": ""}]` are absent, and the benchmark check in
   SC-005 holds (US5).
8. **After OQ-B:** each schema departure gives the outcome recorded in the decision (US6).
