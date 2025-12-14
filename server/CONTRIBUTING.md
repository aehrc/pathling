# How to contribute to Pathling server

This document provides guidance specific to working on the `server` module. For
contribution guidelines for the Pathling core libraries, see the main
[CONTRIBUTING.md](../CONTRIBUTING.md) in the repository root.

## Development dependencies

You will need the following software to build the server:

- Java 21
- Maven 3.9+

## Prerequisites

Before building the server, ensure that:

1. The server's `pom.xml` references the correct Pathling version
2. An up to date version of `library-runtime` is available in your local Maven
   repository

To install the required dependencies:

```bash
mvn clean install -pl library-runtime -am
```

## Building the server

All server build and test commands must be run from inside the `server`
directory:

```bash
cd server
mvn clean install
```

## Running tests

The server test suite includes both unit tests and integration tests:

```bash
cd server
mvn test                    # Run unit tests
mvn verify                  # Run unit and integration tests
```

To run specific tests:

```bash
cd server
mvn test -Dtest=ImportExecutorTest
mvn verify -Dit.test=ImportPnpOperationIT -Dtest=foo -Dsurefire.failIfNoSpecifiedTests=false
```

The last command runs only the specified integration test without running unit
tests. The `-Dtest=foo` parameter specifies a non-existent unit test pattern,
and `-Dsurefire.failIfNoSpecifiedTests=false` prevents failure when no unit
tests match.

## Docker image

The server includes a `docker` profile for building and deploying Docker images.

To build a Docker image locally:

```bash
cd server
mvn clean install -Pdocker
```

To push the image to a remote registry:

```bash
cd server
mvn clean deploy -Pdocker
```

### Configuring Docker repository and tag

You can customise the Docker repository and tag using Maven properties:

```bash
cd server
mvn clean install -Pdocker \
  -Dpathling.fhirServerDockerRepo=myregistry/pathling \
  -Dpathling.fhirServerDockerTag=v1.0.0
```

The default repository is `aehrc/pathling` and the default tag is `latest`.

### Multi-architecture builds

The Docker build automatically creates multi-architecture images for both
`linux/amd64` and `linux/arm64` platforms. This is configured in the Jib
plugin and cannot be overridden via command-line properties.

## Versioning and branching

The server is versioned independently from the Pathling core libraries.
Versioning
follows [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

The branching strategy is based on
[GitHub Flow](https://guides.github.com/introduction/flow/). There are no
long-lived branches, all changes are made via pull requests and will be the
subject of an issue branch that is created from and targeting `main`.

Maven POM versions on `main` are always release versions. Builds are always
verified to be green within CI before merging to main. Merging to main
automatically triggers publishing of artifacts and deployment of the software to
production environments.

## Commit message format

Write commit messages that capture the **objective** of the change, not the
specific implementation details that can be obtained from the diff.

**Structure**:

```
<type>: <succinct description of the objective>

<optional body explaining the why and context>
```

**Types**:

- `fix:` - Bug fixes or resolving warnings/errors
- `feat:` - New features or enhancements
- `refactor:` - Code restructuring without changing behavior
- `docs:` - Documentation updates
- `test:` - Test-related changes
- `chore:` - Build, tooling, or dependency updates

**Guidelines**:

- Focus on **why** the change was needed and **what problem** it solves
- Avoid mentioning specific files, line numbers, or implementation details
- Keep the first line concise (under 72 characters when possible)
- Use the body to provide context if the objective isn't obvious

**Examples**:

Good:

```
fix: Suppress Mockito dynamic agent loading warnings in Java 21

Added JVM flag to suppress warnings about Mockito's inline mock maker
self-attaching. Updated documentation to record Maven test configuration.
```

Poor:

```
fix: Added -XX:+EnableDynamicAgentLoading to pom.xml line 637

Changed the argLine in maven-surefire-plugin configuration.
Updated CLAUDE.md with new section at lines 102-120.
```

## Coding conventions

### Comments

- All comments must use correct grammar and be written as complete sentences.
- Every comment, including single-line comments, must be terminated with a
  period.
- Comments should clearly explain the purpose or logic of the code they
  annotate.
- Avoid redundant comments that restate obvious code; focus on intent,
  rationale, or non-obvious behaviour.
- Update comments when code changes to keep them accurate and relevant.
- TODOs should not be present in code that is being submitted for review.
  If you have a task that you want to complete in the future, please create an
  issue for it.

### Java

- Use meaningful and descriptive names for classes, methods, and variables
  (avoid abbreviations).
- Follow standard Java naming conventions:
    - Classes and interfaces: PascalCase (e.g., `MyClass`)
    - Methods and variables: camelCase (e.g., `myVariable`, `calculateTotal`)
    - Constants: UPPER_SNAKE_CASE (e.g., `MAX_SIZE`)
- Keep methods short and focused on a single responsibility.
- Avoid code duplication; extract common logic into reusable methods.
- Always use braces `{}` for `if`, `else`, `for`, `while`, and `do` statements,
  even for single statements.
- Use `final` for variables, parameters, and methods that should not change.
- Avoid using magic numbers; define constants with meaningful names.
- Avoid the use of inner classes, records and enums - having each class defined
  in its own file is preferred and avoids any implicit dependencies on code
  within the enclosing scope.
- Document public classes and methods
  with [Javadoc comments](https://www.oracle.com/technical-resources/articles/java/javadoc-tool.html).
- Handle exceptions appropriately; do not use empty catch blocks.
- Close resources (e.g., streams, connections) in a `finally` block or use
  try-with-resources.
- Avoid deeply nested code; refactor to improve readability.
- Do not ignore method return values unless intentional and documented.
- Use logging frameworks instead of `System.out` or `System.err` for output.
- Remove unused code, imports, and variables.
- Write unit tests for all public methods and critical logic.
- Avoid hardcoding file paths, URLs, or credentials; use configuration files or
  environment variables.
- Use access modifiers (`private`, `protected`, `public`) appropriately to
  encapsulate data.
- Do not suppress warnings without a clear justification.
- Use nullability annotations (`jakarta.annotation.Nonnull` and
  `jakarta.annotation.Nullable`) on method parameters, return values, and class
  or record fields.
- Do not leave unused or commented-out code in the codebase.
- Ensure code is free of major bugs, vulnerabilities, and code smells as
  reported by SonarQube.

## Documentation

Server documentation is located in the `site/docs/server` directory at the
repository root. When making changes to the server, ensure that relevant
documentation is updated to reflect the changes. This includes:

- Configuration options in `configuration.md`
- Operation descriptions in the `operations` subdirectory
- Authorisation documentation in `authorization.md`
