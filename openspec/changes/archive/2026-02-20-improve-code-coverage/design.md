## Context

The Pathling server module has 64.4% overall coverage. JaCoCo reports from the
v1.1.1 server release (GitHub Actions run 22126219843) match SonarCloud exactly.
The project uses JUnit 5, Mockito, Spring Boot Test, WireMock, and AssertJ.
There are 71 existing test files in the server module following consistent
patterns.

Coverage is lowest in these functional areas (by uncovered line count):

| Area          | Key files                                       | Line coverage |
| ------------- | ----------------------------------------------- | ------------- |
| Server setup  | FhirServer.java (185 lines)                     | 19.4%         |
| Import (PnP)  | ImportPnpExecutor (142), ImportPnpProvider (51) | 10-12%        |
| View export   | ViewDefinitionExportProvider (118)              | 7.1%          |
| Interceptors  | ImportManifestInterceptor (98)                  | 8.0%          |
| Import (bulk) | ImportProvider (54)                             | 14.1%         |
| Security/JWT  | PathlingJwtDecoderBuilder (47)                  | 7.0%          |
| Async         | SparkJobListener (44), JobProvider (107)        | 10-57%        |
| Spark         | Spark.java (30)                                 | 3.1%          |

## Goals / Non-goals

**Goals:**

- Increase server module overall coverage from 64.4% toward 80%.
- Prioritise files with the most uncovered lines for maximum impact.
- Write unit tests that are fast, independent, and maintainable.
- Use JaCoCo HTML reports for local iteration.

**Non-goals:**

- Achieving 100% coverage on all files (some classes like `FhirServer.java` and
  `Spark.java` are heavily integration-dependent and offer diminishing returns).
- Changing production code solely to improve testability (unless the change is a
  genuine improvement).
- Adding integration tests (focus is on unit tests).
- Improving core library coverage (separate effort).

## Decisions

### Test approach: unit tests with Mockito

Use Mockito to mock dependencies and test each class in isolation. This is
consistent with existing test patterns in the codebase and avoids the cost of
spinning up Spring contexts or Spark sessions.

Alternative considered: Spring Boot `@WebMvcTest` slices. Rejected because most
low-coverage files are not web controllers, and slice tests are slower.

### Prioritisation: by uncovered line count

Order work by the number of uncovered lines (descending), since this maximises
coverage improvement per test written. Files with fewer than 5 uncovered lines
are deferred.

Alternative considered: prioritising by percentage. Rejected because a file with
2 lines at 0% is less impactful than a file with 100 lines at 50%.

### Local iteration: JaCoCo HTML reports

Run `mvn verify` in the server module and open
`target/site/jacoco/index.html` to see per-file coverage with highlighted
uncovered lines. This is already configured and requires no changes.

### Phased delivery

Split the work into phases based on testing difficulty:

- **Phase 1**: Simple classes with few dependencies (SecurityError,
  SubmissionRegistry, UpdateProvider, simple provider factories).
- **Phase 2**: Medium-complexity classes requiring HTTP or servlet mocking
  (ImportManifestInterceptor, OidcDiscoveryFetcher, ExportOperationHelper).
- **Phase 3**: Complex classes with deep framework integration
  (ViewDefinitionExportProvider, ImportPnpExecutor, FhirServer, security/JWT
  classes).

## Risks / Trade-offs

- **Mocking depth**: Some classes (e.g., FhirServer) have many dependencies.
  Deep mocking can make tests brittle. Mitigation: focus on behaviour
  verification rather than interaction verification; accept that some
  integration-heavy classes may not reach high coverage through unit tests alone.
- **Test maintenance**: Adding many tests increases maintenance burden.
  Mitigation: keep tests simple and well-structured following existing patterns;
  use shared test fixtures where appropriate.
- **Diminishing returns**: The last 15-20% of coverage is typically the hardest
  to achieve. Mitigation: set a realistic target (80%) and accept that some
  code paths (error handlers, framework callbacks) may remain uncovered.
