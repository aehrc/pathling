# Release checklist

- [ ] Create a release branch (`release/[version]`)
- [ ] Update the version to a SNAPSHOT version for pre-release activities
- [ ] Update all dependencies
- [ ] Get all tests and checks passing on CI ("Test" workflow)
- [ ] Create a Python pre-release ("Python pre-release" workflow, manually triggered)
- [ ] Test snapshot library API, dev Python library release and R package on target Databricks release
- [ ] Update the supported Databricks Runtime versions in the documentation (`site/docs/libraries/installation/databricks.md`)
- [ ] Update licenses (`mvn install -DskipTests -Plicenses`)
- [ ] Update the version to the release version
- [ ] Tag the release (`v[version]`)
- [ ] Merge to main and make sure it is green
- [ ] Create a GitHub release for the tag, describe new features, bug fixes and breaking changes
- [ ] Submit the built R package to CRAN manually

## Updating the version

The version number needs to be changed in the following places:

- [ ] All POM files (version of the main POM, references to parent in all child POMs)
- [ ] `lib/python/Dockerfile`
- [ ] `docs/libraries/installation/spark.md`
