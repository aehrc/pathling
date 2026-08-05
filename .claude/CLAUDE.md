# Pathling

Two independently versioned projects in one repository:

- **The library** — root `pom.xml`. FHIR analytics on Apache Spark. Build order matters:
  `utilities → encoders → terminology → fhirpath → library-api → library-runtime`, with
  `lib/python` and `lib/R` wrapping `library-runtime`.
- **The server** — `server/pom.xml`. A FHIR server, **not** a child of the root POM. It depends on
  `library-runtime` as a published artifact, so library changes reach it only after
  `mvn install -pl library-runtime -am`.

Module descriptions and contribution conventions are in `CONTRIBUTING.md`,
`server/CONTRIBUTING.md`, and `ui/CONTRIBUTING.md`. Do not restate them here.

Version numbers move every release — read them with
`mvn help:evaluate -Dexpression=project.version -q -DforceStdout` rather than trusting any
document.

## Gotchas

Build:

- **Format before compiling.** `spotless:check` runs ahead of compilation, so an unformatted file
  fails the build as an error, not a warning. `mvn spotless:apply -pl <module>`.
- **`cannot access java.util.List`** and similar nonsense errors from `-pl <module>` mean the
  upstream modules are stale, not that the code is broken. Rebuild them with `-am`.
- **`sql-on-fhir/` is a git submodule.** It needs `git submodule update --init` before the
  SQL-on-FHIR compliance tests can run.
- **Python and R pick up Java changes only after the Ivy cache is cleared** — both `cache` and
  `jars`, across every `~/.ivy2*` tree. The SNAPSHOT filename never changes, so Ivy silently reuses
  a stale jar and the tests pass without exercising the new code. See CONTRIBUTING.md for the exact
  sequence, including the `mkdir` step that is required afterwards.

Tests:

- **The YAML exclusion baseline polices itself.** Excluded conformance cases are still executed and
  asserted to fail in the recorded way. After implementing a feature, `Excluded test passed when
  expected outcome was error` is the expected, correct signal that an exclusion is now obsolete —
  not a test failure to work around. See the `pathling-yaml-exclusions` skill.
- **Three fields in the exclusion config do nothing**: an exclude block's `glob` (so every block
  applies to every case file), a rule's `desc` matcher, and the `exclusionsOnly` system property.
  Do not rely on them.

## FHIRPath work

The `fhirpath` module translates FHIRPath into Spark queries. Two facts shape most of the work:

- Every expression evaluates to a `Collection`, which must behave correctly for empty, singleton,
  and multi-item input. Empty propagates through most operations; singleton coercion errors on
  multi-item input rather than taking the first.
- Singular FHIR elements are **scalar** Spark columns and non-singular elements are **array**
  columns. Code correct on one can fail on the other, so both cardinalities need test coverage.

Most new functions are a single `@FhirPathFunction` method on a class in
`fhirpath/.../function/provider/`, registered automatically via `MethodDefinedFunction.mapOf` in
`StaticFunctionRegistry`. Operators, the parser, the evaluation context, and the type system are a
different matter — changes there are inherited by every later feature.

Use these skills rather than duplicating their content here:

| Skill | For |
|---|---|
| `implement-pathling` | Driving a FHIRPath issue to a PR (`/implement-pathling <issue>`) |
| `fhirpath-spec` | The FHIRPath spec and its FHIR bindings — the ground truth on semantics |
| `cache-github-repo` | Pin and locally cache a GitHub repo, e.g. the fhirpath.js reference implementation `fhirpath-spec` consults |
| `fhirpath` | Quick reference for writing expressions; defers to `fhirpath-spec` on semantics |
| `fhirpath-test-designer` | Test matrices and the DSL test surface |
| `pathling-yaml-exclusions` | The conformance exclusion baselines |
| `pathling-fhirpath-review` | Correctness review of a FHIRPath change |
| `fhir-search-spec` | FHIR RESTful search parameters, prefixes, and modifiers |
| `sql-on-fhir` | The SQL-on-FHIR view specification |
| `spark-catalyst` | Spark internals when touching column generation |

## Conventions

Write code that reads like the surrounding code: match its comment density, naming, and idiom.

Beyond `CONTRIBUTING.md`, two project-specific leanings:

- Prefer `Optional` and `Stream` composition over imperative null checks and loops, breaking a chain
  after three or four calls so it stays readable.
- Keep substantial logic in a dedicated `*Logic` class rather than inline in a function provider, as
  `ConversionFunctions` does with `ConversionLogic`.

OpenSpec is not used for ordinary feature work. It is reserved for changes that extend the FHIRPath
framework itself, where the design needs review before code exists — the `implement-pathling` skill
escalates into it when that happens.
