# 07 — Document the legacy export and FR-053; raise the server migration issue

**Spec:** `specs/001-parquet-on-fhir/spec.md` — FR-047, FR-053, FR-064. Old task: T134b (rewritten); decisions 98, 101, 118.

**What to build:** A user reading the docs about legacy data is pointed at the legacy export, and the server's move to the PoF layout has a tracked owner.

- The storage-layout page's compatibility section no longer offers routing or in-place reading of legacy data. It describes the refusal, the legacy export (ticket 06) as the migration path, and FR-053's loud failure for legacy columns that bypass detection.
- Rewrite T134b as a GitHub issue for the server. The server stays pinned to the last pre-PoF release. It moves by migrating its warehouse with the legacy export from `SchemaMigrator`, and maps `UnsupportedLayoutException` to a 500 with the message for a read of its own warehouse, and to a 400 for `$import` of a client-supplied file. The issue also carries decision 118's docs problem: the server docs link to the library's storage-layout page as the server's Parquet format. Before the library release, either the server has moved, or the legacy layout's description goes into the server docs and those links point at it.

**Blocked by:** 06

**Status:** ready-for-agent

- [ ] The storage-layout page and the API docs (Java, Python, R) describe the refusal, the legacy export and FR-053, with no mention of routing.
- [ ] The server issue is raised on GitHub with the content above, and its number is recorded in this ticket's comments.
- [ ] The server-docs link problem is either fixed here or stated in that issue as a release blocker.
