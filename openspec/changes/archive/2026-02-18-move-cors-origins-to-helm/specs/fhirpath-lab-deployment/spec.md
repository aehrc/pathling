## MODIFIED Requirements

### Requirement: Configurable deployment

The Helm chart SHALL support configuration of image, resources, probes,
scheduling, and environment variables.

#### Scenario: Image configuration

- **WHEN** `fhirpathLabApi.image` is set in values
- **THEN** the Deployment uses that container image

#### Scenario: Resource limits

- **WHEN** `fhirpathLabApi.resources` is set with requests and limits
- **THEN** the Deployment container has the specified resource constraints

#### Scenario: Health probes

- **WHEN** the Deployment is created
- **THEN** it includes startup, liveness, and readiness probes targeting the
  `/healthcheck` endpoint

#### Scenario: Environment variable configuration

- **WHEN** `fhirpathLabApi.config` is set with key-value pairs
- **THEN** those values are passed as environment variables to the container

#### Scenario: Scheduling controls

- **WHEN** tolerations, affinity, or nodeSelector are configured
- **THEN** the Deployment pod spec includes the specified scheduling constraints

#### Scenario: Default CORS origins

- **WHEN** the chart is installed with default values
- **THEN** the container receives `CORS_ALLOWED_ORIGINS` set to
  `https://fhirpath-lab.azurewebsites.net,http://localhost:3000`
