# Release checklist

- [ ] Create a release branch (`release/[version]`)
- [ ] Update the version to a SNAPSHOT version for pre-release activities
- [ ] Update all dependencies
- [ ] Get all tests and checks passing on CI ("Build, test and pre-release"
  workflow)
- [ ] Create a Python pre-release ("Python pre-release" workflow, manually
  triggered)
- [ ] Test snapshot library API, dev Python library release and R package on
  target Databricks release
- [ ] Update the supported Databricks Runtime versions in the
  documentation (`site/docs/libraries/installation/databricks.md`)
- [ ] Update licenses (`mvn install -DskipTests -Plicenses`)
- [ ] Update the version to the release version
- [ ] Tag the release (`v[version]`)
- [ ] Merge to main and make sure it is green
- [ ] Create a GitHub release for the tag, describe new features, bug fixes and
  breaking changes
- [ ] Submit the built R package to CRAN manually

## Updating the version

The version number needs to be changed in the following places:

- [ ] All POM files (version of the main POM, references to parent in all child
  POMs)
- [ ] `lib/python/Dockerfile`
- [ ] `docs/libraries/installation/spark.md`

## Helm chart

The Helm chart is versioned independently of the rest of the project. The Helm
chart publishes the chart to the web site based upon the metadata in
the `Chart.yaml` file.

It only currently supports the publishing of the current version of the chart.

No tagging is required to release the chart, it is built and published upon
every execution of the "Deploy site" workflow.
