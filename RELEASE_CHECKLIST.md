# Release checklist

- [ ] Create a release branch (`release/[version]`)
- [ ] Update the version to a SNAPSHOT version for pre-release activities
- [ ] Update SQL on FHIR repository pointer
- [ ] Update all dependencies
- [ ] Get all tests and checks passing on CI ("Pre-release" workflow)
- [ ] Check the status of the Quality Gate
      on [SonarCloud](https://sonarcloud.io/project/branches_list?id=aehrc_pathling)
- [ ] Create a Python pre-release ("Python pre-release" workflow, manually
      triggered)
- [ ] Test snapshot library API, dev Python library release and R package on
      target Databricks release
- [ ] Update the supported Databricks Runtime versions in the
      documentation (`site/docs/libraries/installation/databricks.md`)
- [ ] Run `R CMD check --as-cran --no-examples [package file]` and address any
      warnings or notes
- [ ] Update licenses (`mvn install -DskipTests -Plicenses`)
- [ ] Update copyright statements in all files
- [ ] Update the version to the release version
- [ ] Merge to main and make sure it is green
- [ ] Tag the release (`v[version]`)
- [ ] Create a GitHub release for the tag, describe new features, bug fixes and
      breaking changes
- [ ] Submit the built R package to CRAN manually

## Updating the version

The version number needs to be changed in the following places:

- [ ] All POM files (version of the main POM, references to parent in all child
      POMs)

The following should only be updated for release versions, not snapshots:

- [ ] `examples/java/PathlingJavaApp/pom.xml`
- [ ] `examples/scala/PathlingScalaApp/build.sbt`
- [ ] `lib/python/Dockerfile`
- [ ] `site/docs/libraries/installation/spark.md`
- [ ] `site/docusaurus.config.js`

## Helm chart

The Helm charts are versioned independently of the rest of the project.

To release a Helm chart:

1. Update the version in the relevant `Chart.yaml` file
2. Create and push a tag matching the pattern:
    - Server chart: `helm-server-v[version]` (e.g., `helm-server-v1.2.0`)
    - Cache chart: `helm-cache-v[version]` (e.g., `helm-cache-v1.0.0`)
3. The tag version must match the version in `Chart.yaml`

The GitHub Actions workflow will package the chart and publish it to the Helm
repository at `https://pathling.csiro.au/helm`.

## After release

Update the version to one patch version higher than the release, with a
`-SNAPSHOT` suffix. For example, following the `9.0.0` release, the version is
updated to `9.0.1-SNAPSHOT` for subsequent branching.
