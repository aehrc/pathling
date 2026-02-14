## Context

Pathling currently runs on Apache Spark 4.0.1 with Scala 2.13.15, Delta Lake
4.0.0, and Hadoop 3.4.1. Spark 4.0.2 is available as the latest patch release
in the 4.0.x line.

An upgrade to Spark 4.1.1 was initially attempted but abandoned due to binary
incompatibility with Delta Lake 4.0.0 (the only stable Delta Lake 4.x release
on Maven Central). Specifically, `StreamingRelation` was moved from
`o.a.s.sql.execution.streaming` to `o.a.s.sql.execution.streaming.runtime` in
Spark 4.1, breaking Delta Lake at runtime.

## Goals / non-goals

**Goals:**

- Update Spark from 4.0.1 to 4.0.2 across all modules (core, server,
  language libraries).
- Ensure all existing tests pass with Spark 4.0.2.
- Update any internal APIs that have changed between 4.0.1 and 4.0.2.
- Update documentation to reflect the new Spark version.

**Non-goals:**

- Upgrading to Spark 4.1.x (blocked by Delta Lake binary incompatibility).
- Upgrading Scala version (unless required by Spark 4.0.2).
- Upgrading Delta Lake to a new major version.

## Decisions

### Keep Delta Lake at 4.0.0

Delta Lake 4.0.0 is compatible with Spark 4.0.x. No change needed.

### Keep Hadoop at 3.4.1

Spark 4.0.2 continues to use Hadoop 3.4.x. No change needed.

### Update Catalyst compatibility layer only if needed

The existing `Catalyst.scala` handles both 8-parameter (Spark 3.5.x) and
9-parameter (Spark 4.0-preview) `StaticInvoke` constructors via reflection. If
Spark 4.0.2 introduces a new constructor signature, the adapter must be
extended. If not, no changes are needed.

### Update POM versions in both root and server

The server module has its own `pom.xml` with independently managed version
properties. Both files must be updated.

## Risks / trade-offs

- **Catalyst API changes** — The reflection-based `StaticInvoke` adapter may
  encounter an unsupported constructor signature. -> Mitigation: Add a new case
  to the adapter if needed; the pattern is well-established.
- **Python/R library compatibility** — PySpark and sparklyr may have
  version-specific behaviour. -> Mitigation: Run language library test suites.
- This is a patch-level bump (4.0.1 -> 4.0.2), so the risk of incompatibility
  is very low.
