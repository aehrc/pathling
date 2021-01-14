# How to contribute

Thanks for your interest in contributing to Pathling.

You can find out a bit more about Pathling by reading the [README](README.md)
file within this repository.

## Reporting issues

Issues can be used to:

* Report a defect
* Request a new feature or enhancement
* Ask a question

New issues will be automatically populated with a template that highlights the
information that needs to be submitted with an issue that describes a defect. If
the issue is not related to a defect, please just delete the template and
replace it with a detailed description of the problem you are trying to solve.

## Creating a pull request

Please communicate with us (preferably through creation of an issue) before
embarking on any significant work within a pull request. This will prevent
situations where people are working at cross-purposes.

Your branch should be named `issue/[GitHub issue #]`.

## Versioning and branching

All components of Pathling are versioned as one, according to 
[Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

The "public API" of Pathling is defined as:

1. the FHIR API;
2. all public Java APIs distributed within the JARs (see 
   [Javadocs](https://pathling.csiro.au/docs/java)), and;
3. the configuration schema (see 
   [Configuration](https://pathling.csiro.au/docs/configuration.html)).

The branching strategy is very simple and is based on 
[GitHub Flow](https://guides.github.com/introduction/flow/). There are no 
long-lived branches, all changes are made via pull requests and will be the 
subject of an issue branch that is created from and targeting `master`.

We release frequently, and sometimes we will make use of a short-lived release 
branch to aggregate more than one PR into a new version.

The Maven POM version on `master` will be either a release version, or a version 
of the form `[latest release version]-master-SNAPSHOT`. Builds are always 
verified to be green within CI before merging to master. Merging to master 
automatically triggers publishing of artifacts and deployment of the software to 
production environments such as the Pathling web site and sandbox instance.

### Coding conventions

This repository uses the 
[Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).

## Code of conduct

Before making a contribution, please read the
[code of conduct](CODE_OF_CONDUCT.md).
