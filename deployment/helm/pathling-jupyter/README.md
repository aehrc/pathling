# Pathling Jupyter Helm chart

Installs Pathling Jupyter Notebook into a [Kubernetes](https://kubernetes.io/) cluster. This chart deploys a Jupyter notebook environment with Pathling pre-installed, allowing interactive data analysis and exploration using the Pathling Python library.

## Features

- Jupyter notebook environment with Pathling Python library pre-installed
- Flexible Apache Spark configuration with support for connecting to external Spark clusters
- Services for Jupyter UI and Spark UI
- Optional persistent storage for notebooks
- Customisation of resource requests and limits
- Configuration of volumes and volume mounts for data access
- Tolerations and affinity for control over pod scheduling
- Secret configuration for sensitive values such as authentication tokens

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+

## Installation

To install the chart with the default configuration:

```bash
helm install pathling-jupyter ./deployment/helm/pathling-jupyter
```

To install with custom values:

```bash
helm install pathling-jupyter ./deployment/helm/pathling-jupyter -f my-values.yaml
```

## Configuration

This is the list of the configuration values that the chart supports, along with their default values.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `pathlingJupyter.image` | The Pathling Jupyter Docker image to use | `aehrc/pathling-notebook:latest` |
| `pathlingJupyter.resources.requests.cpu` | The CPU request for the Jupyter pod | `1` |
| `pathlingJupyter.resources.requests.memory` | The memory request for the Jupyter pod | `2G` |
| `pathlingJupyter.resources.limits.memory` | The memory limit for the Jupyter pod | `4G` |
| `pathlingJupyter.deployment.strategy` | The deployment strategy to use | `RollingUpdate` |
| `pathlingJupyter.deployment.imagePullPolicy` | The image pull policy to use | `IfNotPresent` |
| `pathlingJupyter.service.type` | The service type for Jupyter UI | `LoadBalancer` |
| `pathlingJupyter.volumes` | A list of volumes to mount in the pod | `[]` |
| `pathlingJupyter.volumeMounts` | A list of volume mounts to mount | `[]` |
| `pathlingJupyter.tolerations` | A list of tolerations to apply to the pod | `[]` |
| `pathlingJupyter.affinity` | Affinity to apply to the pod | `~` |
| `pathlingJupyter.config` | A map of configuration environment variables | `{}` |
| `pathlingJupyter.secretConfig` | A map of secret configuration environment variables | `{}` |
| `pathlingJupyter.spark` | A map of Apache Spark configuration settings | `{}` |
| `pathlingJupyter.persistence.enabled` | Enable persistent storage for notebooks | `false` |
| `pathlingJupyter.persistence.size` | Size of the persistent volume | `10Gi` |
| `pathlingJupyter.persistence.storageClass` | Storage class for the persistent volume | `~` |

## Example configurations

### Basic deployment

This configuration deploys Jupyter with default settings using local Spark mode.

```yaml
pathlingJupyter:
  image: aehrc/pathling-notebook:latest
  resources:
    requests:
      cpu: 1
      memory: 2G
    limits:
      memory: 4G
```

### Connected to Pathling Spark cluster

This configuration connects the Jupyter notebook to an existing Pathling server's Spark cluster. This is useful when you want to leverage the compute resources of a separate Pathling deployment.

```yaml
pathlingJupyter:
  image: aehrc/pathling-notebook:latest
  resources:
    requests:
      cpu: 1
      memory: 2G
    limits:
      cpu: 2
      memory: 4G
  spark:
    spark.master: "spark://pathling-driver:7077"
```

When configured this way, the Jupyter notebook will connect to the Pathling Spark driver service and submit jobs to the Pathling cluster instead of running them locally.

### With persistent storage

This configuration enables persistent storage for notebooks, ensuring that work is preserved across pod restarts.

```yaml
pathlingJupyter:
  image: aehrc/pathling-notebook:latest
  persistence:
    enabled: true
    size: 20Gi
```

### With authentication

This configuration secures the Jupyter notebook with a token-based authentication.

```yaml
pathlingJupyter:
  image: aehrc/pathling-notebook:latest
  secretConfig:
    JUPYTER_TOKEN: "my-secure-token"
```

Users will need to provide the token when accessing the Jupyter interface.

### With custom data volumes

This configuration mounts additional volumes for accessing data within notebooks.

```yaml
pathlingJupyter:
  image: aehrc/pathling-notebook:latest
  volumes:
    - name: data
      hostPath:
        path: /mnt/data
  volumeMounts:
    - name: data
      mountPath: /data
      readOnly: true
```

### Advanced Spark configuration

This configuration demonstrates advanced Spark settings, including executor memory and cores.

```yaml
pathlingJupyter:
  image: aehrc/pathling-notebook:latest
  resources:
    requests:
      cpu: 2
      memory: 4G
    limits:
      cpu: 4
      memory: 8G
  spark:
    spark.master: "spark://pathling-driver:7077"
    spark.executor.memory: "2G"
    spark.executor.cores: "2"
    spark.driver.memory: "2G"
```

## Accessing Jupyter

Once deployed, the Jupyter notebook will be accessible via the LoadBalancer service. To get the external IP or hostname:

```bash
kubectl get service pathling-jupyter
```

Navigate to `http://<EXTERNAL-IP>:8888` in your web browser to access the Jupyter interface.

## Upgrading

To upgrade an existing release:

```bash
helm upgrade pathling-jupyter ./deployment/helm/pathling-jupyter -f my-values.yaml
```

## Uninstalling

To uninstall the chart:

```bash
helm uninstall pathling-jupyter
```

Note that persistent volume claims are not automatically deleted. To remove them:

```bash
kubectl delete pvc pathling-jupyter-pvc
```
