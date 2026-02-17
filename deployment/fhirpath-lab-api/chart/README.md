# FHIRPath Lab API Helm chart

A Helm chart for deploying the FHIRPath Lab API server on Kubernetes.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.x

## Installation

```bash
helm install fhirpath-lab-api ./deployment/fhirpath-lab-api/chart/
```

With custom values:

```bash
helm install fhirpath-lab-api ./deployment/fhirpath-lab-api/chart/ \
  -f my-values.yaml
```

## Configuration

| Parameter                        | Description                             | Default                                 |
| -------------------------------- | --------------------------------------- | --------------------------------------- |
| `fhirpathLabApi.image`           | Container image                         | `ghcr.io/aehrc/fhirpath-lab-api:latest` |
| `fhirpathLabApi.imagePullPolicy` | Image pull policy                       | `Always`                                |
| `fhirpathLabApi.replicas`        | Number of replicas                      | `1`                                     |
| `fhirpathLabApi.service.type`    | Kubernetes service type                 | `ClusterIP`                             |
| `fhirpathLabApi.service.port`    | Service port                            | `8080`                                  |
| `fhirpathLabApi.resources`       | CPU/memory resource requests and limits | `{}`                                    |
| `fhirpathLabApi.config`          | Environment variables for the container | `{}`                                    |
| `fhirpathLabApi.tolerations`     | Pod tolerations                         | `[]`                                    |
| `fhirpathLabApi.affinity`        | Pod affinity rules                      | `{}`                                    |
| `fhirpathLabApi.nodeSelector`    | Pod node selector                       | `{}`                                    |

## Examples

### Basic deployment with resource limits

```yaml
fhirpathLabApi:
    resources:
        requests:
            memory: "4Gi"
            cpu: "1"
        limits:
            memory: "4Gi"
```

### Additional CORS origins

```yaml
fhirpathLabApi:
    config:
        CORS_ALLOWED_ORIGINS: "https://my-app.example.com,https://staging.example.com"
```

## Uninstalling

```bash
helm uninstall fhirpath-lab-api
```
