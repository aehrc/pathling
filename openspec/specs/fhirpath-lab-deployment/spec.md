## ADDED Requirements

### Requirement: Helm chart for Kubernetes deployment

A Helm chart SHALL be provided at `deployment/fhirpath-lab-api/chart/` for
deploying the FHIRPath Lab API server to any Kubernetes cluster. The chart
SHALL not contain assumptions about a specific cluster or environment.

#### Scenario: Chart structure

- **WHEN** the Helm chart is inspected
- **THEN** it contains `Chart.yaml`, `values.yaml`, `values.schema.json`,
  `README.md`, and a `templates/` directory with deployment and service
  templates

#### Scenario: Helm install

- **WHEN** the chart is installed with default values and a valid image
- **THEN** it creates a Deployment, a Service, and the server becomes
  accessible on the configured port

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

### Requirement: Service configuration

The Helm chart SHALL create a Kubernetes Service for the FHIRPath Lab API
server.

#### Scenario: Default service

- **WHEN** the chart is installed with default values
- **THEN** a ClusterIP service is created exposing the server port

#### Scenario: Configurable service type

- **WHEN** `fhirpathLabApi.service.type` is set to `NodePort` or
  `LoadBalancer`
- **THEN** the Service uses the specified type

### Requirement: GitHub Actions CI/CD workflow

A GitHub Actions workflow SHALL be provided for building and publishing the
Docker image. The workflow SHALL not perform deployment to any specific cluster.

#### Scenario: Workflow triggers

- **WHEN** code is pushed to `main` affecting `fhirpath-lab-api/` or its
  deployment files
- **THEN** the workflow builds a Docker image and pushes it to the container
  registry

#### Scenario: Manual trigger

- **WHEN** the workflow is triggered manually
- **THEN** it builds and publishes the Docker image
