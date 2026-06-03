# Core libraries

- [ ] Create a release branch (`release/[version]`)
- [ ] Update the version to a SNAPSHOT version for pre-release activities
- [ ] Update SQL on FHIR repository pointer
- [ ] Check that all changes have been adequately reflected in the
      documentation (`site/docs`)
- [ ] Run Trivy and address any reported vulnerabilities (see `./.claude/commands/trivy-scan.md`)
- [ ] Get all tests and checks passing on CI ("Pre-release" workflow)
- [ ] Check the status of the Quality Gate on
      [SonarCloud](https://sonarcloud.io/project/branches_list?id=aehrc_pathling)
- [ ] Create a Python pre-release ("Python pre-release" workflow, manually
      triggered)
- [ ] Test snapshot library API, dev Python library release and R package on
      target Databricks release (see `./.claude/commands/test-databricks-release.md`)
- [ ] Update licenses if there have been dependency updates (`mvn install -DskipTests -Plicenses`)
- [ ] Update the version to the release version
- [ ] Merge to main and make sure the documentation site gets deployed properly
- [ ] Tag the release (`v[version]`)
- [ ] Draft a GitHub release for the tag, describe new features, bug fixes and
      breaking changes (see `./.claude/commands/draft-release.md`)
- [ ] Publish the release and verify that it succeeds
- [ ] Submit the built R package to CRAN manually

## Updating the core library version

The version number needs to be changed in the following places:

- `pom.xml`
- `terminology/pom.xml`
- `library-api/pom.xml`
- `utilities/pom.xml`
- `encoders/pom.xml`
- `fhirpath/pom.xml`
- `lib/python/pom.xml`
- `lib/R/pom.xml`
- `site/pom.xml`
- `library-runtime/pom.xml`
- `benchmark/pom.xml`

The following should only be updated for release versions, not snapshots:

- [ ] `examples/java/PathlingJavaApp/pom.xml`
- [ ] `examples/scala/PathlingScalaApp/build.sbt`
- [ ] `lib/python/Dockerfile`
- [ ] `site/docs/libraries/installation/spark.md`
- [ ] `site/docusaurus.config.js`

## After release

Update the version to one patch version higher than the release, with a
`-SNAPSHOT` suffix. For example, following the `9.0.0` release, the version is
updated to `9.0.1-SNAPSHOT` for subsequent branching.

# Server

The server is versioned independently of the core libraries, following Semantic
Versioning. Its Docker image is published to GitHub Container Registry
(`ghcr.io/aehrc/pathling`).

- [ ] Create a release branch (`release/server/[version]`)
- [ ] Update the server version to a SNAPSHOT version for pre-release activities
      (`server/pom.xml`)
- [ ] Confirm that the server points to a release (non-SNAPSHOT) version of the
      core libraries (`pathling.version` in `server/pom.xml`). The server must
      not be released against a SNAPSHOT version
- [ ] Verify that this core library version has already been published to
      [Maven Central](https://central.sonatype.com/artifact/au.csiro.pathling/library-runtime)
      before proceeding with the server release
- [ ] Check that all changes have been adequately reflected in the documentation
      (`site/docs/server`)
- [ ] Run Trivy against both the server and UI and address any reported
      vulnerabilities (see `./.claude/commands/trivy-scan.md`)
- [ ] Get all tests and checks passing on CI ("Server pre-release" workflow,
      triggered by pushing to the `release/server/*` branch). A successful build
      publishes a SNAPSHOT Docker image tagged with the project version.
- [ ] Check the status of the Quality Gate on
      [SonarCloud](https://sonarcloud.io/project/branches_list?id=aehrc_pathling_server)
      (`aehrc_pathling_server` project)
- [ ] Update the server version to the release version (`server/pom.xml`)
- [ ] Merge to main
- [ ] Tag the release (`server-v[version]`)
- [ ] Draft a GitHub release for the tag, describing new features, bug fixes and
      breaking changes (see `./.claude/commands/draft-release.md`)
- [ ] Publish the release and verify that it succeeds. Publishing triggers the
      "Release server" workflow, which runs the server and UI security scans and
      the Sonar analysis, then builds, tests and pushes the multi-architecture
      Docker image to `ghcr.io/aehrc/pathling` (tagged with both `latest` and the
      project version)
- [ ] Verify that the workflow succeeds and the image is available

The server Helm chart is versioned and released separately (see the Helm chart
section below).

## After release

Update the server version to one patch version higher than the release, with a
`-SNAPSHOT` suffix (`server/pom.xml`).

# FHIRPath Lab API

The FHIRPath Lab API is versioned independently (`fhirpath-lab-api/pyproject.toml`)
and is released continuously. Any change merged to `main` that touches
`fhirpath-lab-api/` or `deployment/fhirpath-lab-api/` automatically rebuilds and
publishes the Docker image to `ghcr.io/aehrc/fhirpath-lab-api`. There is no
separate release branch or git tag.

- [ ] Update the version (`fhirpath-lab-api/pyproject.toml`)
- [ ] Check that all changes have been adequately reflected in the documentation
      (`fhirpath-lab-api/README.md`)
- [ ] Run Trivy and address any reported vulnerabilities
      (`fhirpath-lab-api/.trivyignore`)
- [ ] Get the tests passing locally (`uv sync --extra dev && uv run pytest -v`)
- [ ] Merge to main. The "Release FHIRPath Lab API" workflow runs automatically
      when files under `fhirpath-lab-api/` or `deployment/fhirpath-lab-api/`
      change. It runs the security scan and tests, then builds and pushes the
      multi-architecture Docker image to `ghcr.io/aehrc/fhirpath-lab-api:latest`
- [ ] Verify that the workflow succeeds and the image is available

The FHIRPath Lab API Helm chart (`deployment/fhirpath-lab-api/chart`) is
versioned independently. Bump its version in `Chart.yaml` when the chart itself
changes.

# Helm charts

The Helm charts are versioned independently of the rest of the project.

To release a Helm chart:

- [ ] Update the version in the relevant `Chart.yaml` file
- [ ] Create and push a tag matching the pattern:
    - Server chart: `helm-server-v[version]` (e.g., `helm-server-v1.2.0`)
    - Cache chart: `helm-cache-v[version]` (e.g., `helm-cache-v1.0.0`)
    - The tag version must match the version in `Chart.yaml`
- [ ] Verify that the GitHub Actions workflow packages the chart and publishes
      it to the Helm repository at `https://pathling.csiro.au/helm`.
