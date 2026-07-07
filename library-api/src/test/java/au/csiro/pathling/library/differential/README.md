# Differential parity suite

`DifferentialParityTest` validates success criterion **SC-001**: that local
terminology mode returns the same results as a reference FHIR terminology server
loaded with the same content, across all seven terminology functions.

The suite is tagged `differential` and is skipped by default. It runs only when
a reference server and a local store are supplied through system properties, so
it never runs in the standard build or in CI.

## What it compares

For each query in the corpus it evaluates the same column expression twice - once
with a `LOCAL`-mode context reading the store, once with a `SERVER`-mode context
pointing at the reference server - and asserts the results are identical. Because
both sides run the same expression, the test compares two implementations of
identical semantics rather than checking against hardcoded answers.

The corpus covers:

- `member_of` against SNOMED implicit value sets (`isa/`, `refset/`), the
  supported ECL subset (`ecl/`), and a VCL URL (`http://fhir.org/VCL`);
- `subsumes` and `subsumed_by` across the concept hierarchy;
- `display` (preferred term selection);
- `designation` (synonyms);
- `property_of` (`parent`, `inactive`);
- `translate` through the SNOMED implicit SAME AS association concept map
  (`?fhir_cm=`).

Set-valued results (designations, properties, translation targets) are compared
as sorted sets, so ordering differences between the two engines do not cause
spurious failures.

## Loading identical content into both sides

The two sides must hold the **same SNOMED CT edition and version**. The corpus
leaves all references unversioned, so each side resolves them against its
single loaded SNOMED release; if the releases differ, display and designation
comparisons will produce spurious mismatches.

1. **Reference server.** Point `pathling.test.txServerUrl` at a FHIR terminology
   server that already has this edition loaded. Confirm it is present:

    ```bash
    curl -s "$TX/CodeSystem?system=http://snomed.info/sct&_elements=version" | jq
    ```

2. **Local store.** Either import the matching RF2 archive yourself with the CLI:

    ```bash
    pathling import-snomed /path/to/SnomedCT_*.zip /data/tx-store
    ```

    or let the suite provision the store on first run by also supplying
    `pathling.test.rf2Path` - if the store is empty, the suite imports that archive
    (and logs the import duration) before evaluating the corpus.

## Running

```bash
cd library-api
mvn test -Dtest=DifferentialParityTest -Dgroups=differential \
  -Dpathling.test.txServerUrl=http://tx.example.org/fhir \
  -Dpathling.test.local.storagePath=/data/tx-store \
  -Dpathling.test.rf2Path=/path/to/SnomedCT_*.zip
```

Omit `pathling.test.rf2Path` once the store has been built. To validate a
different edition, load that edition on both sides.

## Interpreting failures

A failure lists every mismatched query with its local and remote values, so an
entire category can be triaged from one run. Mismatches usually indicate one of:

- a genuine parity gap in local mode (a real defect to fix);
- a difference in the SNOMED edition or version loaded on each side (align them);
- a display/designation difference driven by the default language reference set
  (pin `acceptLanguage` consistently, or align the editions).

Copyright © 2025, Commonwealth Scientific and Industrial Research Organisation
(CSIRO) ABN 41 687 119 230.
