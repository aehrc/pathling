# 11 — Annotation registry, per-annotation toggles and SchemaConfiguration

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-021, FR-022; ADR-0003. Old tasks: T063, T055; decisions 69, 84, 121.

**What to build:** The framework that ingest uses to emit annotations, with each kind on by default and individually disableable. It emits no annotation kind of its own. Those land in tickets 12–14.

- **Recreate `SchemaConfiguration` in `fhir-schema`.** T033c deleted it. It sits beside `EncodingConfiguration`, not on it, because `encoders` must not change (FR-051). It carries the per-annotation toggles, and ticket 18 adds the schema mode. A kind carried in more than one field is governed by one switch.
- **Decide before writing the first processor:**
  1. **Where the processors live (decision 84).** The transform and its helpers are package-private in `io`, so a processor cannot live in a sub-package without widening visibility, which decision 84 forbids. Move the processors rather than widen the transform's visibility.
  2. **The width of a dense schema's annotations.** Either every described element carries one, or only populated ones do. A dense `Observation` is already 1371 leaf columns. This is decided here, without being exercised until ticket 18.
- **Thread the configuration through `PathlingContext`** in Java, Python and R.

**Blocked by:** None — can start immediately

**Status:** ready-for-human

- [ ] Both decisions are recorded, and the owner approves them before processors are written.
- [ ] The registry runs enabled processors and skips disabled ones. A test registers a stub processor per kind and shows that disabling one does not affect the others (T055).
- [ ] The configuration is reachable from Java, Python and R, with documented defaults of all on.
- [ ] Nothing about stored output changes yet, because no real processors are registered.
- [ ] If ticket 29 has landed, a dataset ingested with every annotation switch off still carries the layout column.
