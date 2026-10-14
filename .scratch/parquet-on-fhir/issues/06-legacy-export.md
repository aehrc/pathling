# 06 — Legacy export: legacy Parquet/Delta to FHIR NDJSON, as a component and CLI

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-064, FR-051, SC-012, US9. Replaces T134a.

**What to build:** A user with a warehouse in the legacy layout can convert it to FHIR NDJSON, then re-import it through the PoF path. The export is a reusable component and a command-line tool, in a module of its own. It depends only on `encoders`, decoding with the legacy encoder, and is outside `library-runtime` and the language bindings. It reads legacy-layout Parquet and Delta tables, one resource type each, and writes NDJSON per type. Its losses are documented: for example, the legacy layout's fixed-scale decimals, and anything the legacy encoder did not store.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] The new module is not a dependency of `library-runtime`, Python or R. It depends on `encoders` and nothing newer in the core chain.
- [ ] The component exports a legacy Parquet table and a legacy Delta table to NDJSON.
- [ ] The CLI takes a source location, a format and an output location, and exits non-zero with a clear message on a non-legacy or unreadable source.
- [ ] SC-012 test: a table written by the legacy encoder, exported and re-imported through `FhirReader`, holds every resource of the source, equal to it except for the documented losses.
- [ ] The losses are listed in the module's documentation.
- [ ] `encoders` is unmodified in its encoding behaviour (FR-051).
