# Pathling CI runner image

Docker image containing all dependencies required to build and test Pathling in
CI environments.

## Included tools

| Category | Tool          | Version |
| -------- | ------------- | ------- |
| Java     | Azul Zulu JDK | 21      |
| Build    | Maven         | 3.9.9   |
| R        | R             | 4.x     |
| R        | TinyTeX       | latest  |
| R        | Pandoc        | 3.1.13  |
| Python   | Python 3      | system  |
| Python   | uv            | latest  |
| JS       | Bun           | latest  |
| JS       | Playwright    | latest  |
| DevOps   | Helm          | latest  |
| DevOps   | Trivy         | 0.58.x  |
| DevOps   | ORAS          | 1.2.x   |
| DevOps   | AWS CLI       | v2      |
| DevOps   | GitHub CLI    | latest  |

## Building locally

```bash
cd dev-image
docker build -t pathling-runner .
```

For buildx (used in CI):

```bash
docker buildx build --platform linux/amd64 -t pathling-runner .
```

## Verifying the build

```bash
docker run --rm pathling-runner bash -c "\
  java -version && \
  mvn -version && \
  R --version && \
  python3 --version && \
  bun --version && \
  gh --version"
```

## Using in CI

The image is published to `ghcr.io/aehrc/pathling-runner` and can be used in
GitHub Actions workflows:

```yaml
jobs:
    build:
        runs-on: ubuntu-latest
        container:
            image: ghcr.io/aehrc/pathling-runner:latest
        steps:
            - uses: actions/checkout@v4
            - run: mvn clean install
```

## Environment variables

The following environment variables are pre-configured:

- `JAVA_HOME=/usr/lib/jvm/zulu21`
- `MAVEN_HOME=/opt/maven`
- `MAVEN_OPTS` - includes flags for Java 21 compatibility
- `LANG=en_US.UTF-8`
