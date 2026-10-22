# 36 — A public XML sink and source

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-045. Decision 84. Ticket 32 built the `io`-level sink and source this exposes.

**What to build:** Line-delimited FHIR XML is written and read through the library's public data sinks and sources, as NDJSON is.

- `DataSinkBuilder` gains `xml()`, mirroring `ndjson()`, over `FhirXmlWriter.write(type, stored, path, mode)`: one file set per resource type, one document per line, a line feed left in element content written as `&#xa;`.
- `DataSourceBuilder` gains `xml()`, mirroring `ndjson()`, over `FhirXmlReader.read(type, path)`, which reads what the sink writes.
- Python and R wrappers, beside their NDJSON ones.
- Tests in Java, Python and R: what the sink writes reads back through the source, including a narrative holding a line feed, and the file naming and save modes behave as NDJSON's do.
- The site's data sources and sinks pages describe the format.

**Out of scope:** XML bundles as a source. `bundles()` already takes XML.

**Blocked by:** 32

**Status:** ready-for-agent

- [ ] `DataSinkBuilder.xml()` writes line-delimited FHIR XML, one document per line, with the same file naming and save modes as `ndjson()`.
- [ ] `DataSourceBuilder.xml()` reads it back, and what is stored equals what was written.
- [ ] A narrative holding a line feed survives the sink and the source.
- [ ] Python and R expose both, with tests passing after the Ivy cache is cleared.
- [ ] The site documents the format beside NDJSON.

## Comments

### Moved from ticket 32 (owner, 2026-10-22)

Ticket 32 built the line-delimited XML sink and source in `io` only, as `FhirXmlWriter.write(type, stored, path, mode)` and `FhirXmlReader.read(type, path)`. Adding them to the public sinks and sources, with Python and R wrappers, was judged beyond what that ticket implied. The owner moved it here.
