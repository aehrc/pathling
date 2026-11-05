# 24 — Remove or restore the benchmark's unreachable NDJSON source

**Spec:** `specs/001-parquet-on-fhir/spec.md` — SC-007. Old task: T124h.

**What to build:** The benchmark's source parameter was narrowed to `delta`, leaving the `ndjson` branch and `initialiseNdjsonSource` unreachable and untested. Either remove them, or restore `ndjson` to the parameter.

**Blocked by:** None — can start immediately

**Status:** done

- [x] No unreachable source branch remains. The benchmark compiles and a short run succeeds.

## Comments

### Implementation (branch `issue/2830-small-tickets`)

Removed rather than restored, because a query over an NDJSON source encodes the data lazily within
every query, which is the conflation the benchmark's split into encode, decode, planning and
execution exists to remove.

The ticket was half right. The `ndjson` branch of `setup()` was unreachable, but
`initialiseNdjsonSource` was not: the Delta source calls it to encode the data before writing it. So
the `sourceType` parameter, its setter and the branch are removed. The helper is kept and renamed
`encodeResources`, after what it does. Its documentation now says that its result is only written
out, never queried.

A result from before this change carries `sourceType = delta` among its parameters, and one from
after carries no such parameter. The view is the only parameter of the query pair now. The scripts
that compare revisions do not key on parameters.

- The benchmark compiles. A short run of the query pair, `-p view=PatientDemographics -f 1 -wi 0 -i 1`,
  succeeds.
