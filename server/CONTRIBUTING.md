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

The default repository is `ghcr.io/aehrc/pathling` and the default tag is
`latest`.

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

## Documentation

Server documentation is located in the `site/docs/server` directory at the
repository root. When making changes to the server, ensure that relevant
documentation is updated to reflect the changes. This includes:

- Configuration options in `configuration.md`
- Operation descriptions in the `operations` subdirectory
- Authorisation documentation in `authorization.md`
