# 18 — Dense schema mode, with bounds reporting and streaming encode

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-009, FR-010, FR-017, FR-043, FR-044. Old tasks: T031a, T031e, T031d, T033, T033a, T033d, T134f; decisions 59, 68, 69, 87, 94, 121.

**What to build:** A user can choose the dense schema, which is derived from the definitions and bounded by the nesting, extension and open-type options, and can then encode a stream.

- **A bounded cursor beside the fitted one (T031a).** Extract the traversal strategy interface and implement a bounded cursor over it. The cursor is the only place FR-044's bounds live.
  - The extension switch and the open-type set cut entries. Only nesting cuts descent.
  - `fieldOrder()` still reports the full order.
  - The cursor is not memoised. Its state stays out of the shared canonical-structure memo.
- **Typed nulls for missing children (T031e).** Where the observed schema lacks a child, emit a typed null built structurally. Never materialise a `StructType` for this (decision 68). Assert that FR-011's post-order prune still holds.
- **The schema mode on `SchemaConfiguration` (T033, T033a).** Add the mode, default fitted, and thread it onto `PathlingContext`. Rewrite the docs for `max_nesting_level`, `enable_extensions` and `enabled_open_types` in Python, R and Javadoc to say they bound only the dense mode. Document the mode on the schema docs page, revising "The schema is fitted to the data".
- **Bounds reporting (T134f).** Build it here rather than raising a follow-up. Reinstate decision 59's separation of a bound from undescribed content.
- **Streaming encode (T033d).** Restore streaming `encode` and `encodeBundle` in dense mode. Rewrite `PathlingContextTest.testEncodeResourceStream` from asserting the refusal to counting encoded resources, and restore the Kafka example in the streaming docs.
- **Annotations in dense mode.** Implement the dense-mode annotation width ticket 11 decided.
- **On the interim ingest.** Build on the ingest of decision 134: Spark's reader and the generated transform for JSON, and the `from_xml` route of ticket 32 for XML and bundles. If ticket 16 later replaces the ingest, it carries dense mode over.

**Blocked by:** 32. Ticket 11, which decides the dense-mode annotation width this ticket implements.

**Status:** ready-for-agent

- [ ] The cursor terminates on a self-recursive type at its bound, and a reference within a reference is not expanded (T031d).
- [ ] The configuration test shows the mode is readable and defaults to fitted (T033a).
- [ ] Content dropped by a bound is reported as a finding (FR-017).
- [ ] A streaming encode of `Patient` and `Condition` in dense mode succeeds and counts correctly. In fitted mode it is still refused with a clear message.
- [ ] A dataset ingested in dense mode carries the layout column of ticket 29.
- [ ] The docs are updated as described.
