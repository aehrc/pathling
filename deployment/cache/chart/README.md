# pathling-cache

A Helm chart for deploying a Varnish-based frontend cache optimised for use with
Pathling.

## Features

- HTTP caching layer to reduce load on Pathling servers
- Configurable backend connection to Pathling
- Support for cache bypass via standard HTTP cache control headers
- Kubernetes-native deployment with health checks

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- A running Pathling server to cache

## Installation

```bash
helm install my-cache ./chart \
  --set pathlingCache.pathlingHost=pathling-service \
  --set pathlingCache.pathlingPort=8080
```

## Configuration

| Parameter                       | Description                                | Default                               |
| ------------------------------- | ------------------------------------------ | ------------------------------------- |
| `pathlingCache.image`           | Container image to use                     | `ghcr.io/aehrc/pathling-cache:latest` |
| `pathlingCache.imagePullPolicy` | Image pull policy                          | `IfNotPresent`                        |
| `pathlingCache.replicas`        | Number of replicas to deploy               | `1`                                   |
| `pathlingCache.pathlingHost`    | Hostname of the Pathling server (required) | `~`                                   |
| `pathlingCache.pathlingPort`    | Port of the Pathling server (required)     | `~`                                   |
| `pathlingCache.service.type`    | Kubernetes service type                    | `ClusterIP`                           |
| `pathlingCache.service.port`    | Service port                               | `80`                                  |
| `pathlingCache.resources`       | CPU/memory resource requests and limits    | `{}`                                  |
| `pathlingCache.tolerations`     | Pod tolerations                            | `[]`                                  |
| `pathlingCache.affinity`        | Pod affinity rules                         | `{}`                                  |
| `pathlingCache.nodeSelector`    | Node selector labels                       | `{}`                                  |

## Examples

### Basic installation

```bash
helm install pathling-cache ./chart \
  --set pathlingCache.pathlingHost=pathling \
  --set pathlingCache.pathlingPort=8080
```

### With resource limits

```bash
helm install pathling-cache ./chart \
  --set pathlingCache.pathlingHost=pathling \
  --set pathlingCache.pathlingPort=8080 \
  --set pathlingCache.resources.requests.memory=128Mi \
  --set pathlingCache.resources.requests.cpu=100m \
  --set pathlingCache.resources.limits.memory=256Mi \
  --set pathlingCache.resources.limits.cpu=200m
```

### Using a values file

Create a `my-values.yaml` file:

```yaml
pathlingCache:
    pathlingHost: "pathling-server"
    pathlingPort: "8080"
    replicas: 2
    resources:
        requests:
            memory: "128Mi"
            cpu: "100m"
        limits:
            memory: "256Mi"
            cpu: "200m"
```

Then install:

```bash
helm install pathling-cache ./chart -f my-values.yaml
```

## Uninstalling

```bash
helm uninstall pathling-cache
```

## Copyright

Copyright © 2026, Commonwealth Scientific and Industrial Research Organisation (
CSIRO)
ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.
