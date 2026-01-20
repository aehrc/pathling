# Kubernetes

[Kubernetes](https://kubernetes.io/) is an open-source system for automating deployment, scaling, and management of containerized applications. Support for deploying Pathling on Kubernetes is provided via a [Helm](https://helm.sh/) chart.

The Helm chart includes the following features:

* Support for [startup, liveness and readiness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/) powered by the [Spring Boot Actuator](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html) endpoint
* Services for the FHIR API, Actuator management API, Spark UI, driver endpoint and block manager endpoint
* Support for the [Spark Kubernetes cluster manager](https://spark.apache.org/docs/latest/running-on-kubernetes.html), including a service account, role and role binding to allow it to manage executor pods
* Customisation of [resource requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
* Configuration of [volumes and volume mounts](https://kubernetes.io/docs/concepts/storage/volumes/)
* [Image pull secrets](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/) for private Docker registries
* [Tolerations and affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/) for control over pod scheduling
* [Secret](https://kubernetes.io/docs/concepts/configuration/secret/) config for sensitive values
* [Security context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/) for pod security settings

## Installation[​](#installation "Direct link to Installation")

To install the chart, run the following commands:

```
# Add the Pathling Helm repository.
helm repo add pathling https://pathling.csiro.au/helm

# Get the latest information about charts from the repository.
helm repo update

# Install the Pathling server chart as a release named `pathling`, with the
# default values.
helm install pathling pathling/pathling
```

## Values[​](#values "Direct link to Values")

This is the list of the configuration values that the chart supports, along with their default values.

| Key                                   | Default                         | Description                                                                                                                                                                 |
| ------------------------------------- | ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pathling.image`                      | `ghcr.io/aehrc/pathling:latest` | The Pathling Docker image to use                                                                                                                                            |
| `pathling.resources.requests.cpu`     | `2`                             | The CPU request for the Pathling pod                                                                                                                                        |
| `pathling.resources.requests.memory`  | `4G`                            | The memory request for the Pathling pod                                                                                                                                     |
| `pathling.resources.limits.memory`    | `4G`                            | The memory limit for the Pathling pod                                                                                                                                       |
| `pathling.resources.maxHeapSize`      | `2800m`                         | The maximum heap size for the JVM, should usually be about 75% of the available memory                                                                                      |
| `pathling.additionalJavaOptions`      | `-Duser.timezone=UTC`           | Additional Java options to pass to the JVM                                                                                                                                  |
| `pathling.deployment.strategy`        | `Recreate`                      | The [deployment strategy](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#strategy) to use                                                            |
| `pathling.deployment.imagePullPolicy` | `Always`                        | The [image pull policy](https://kubernetes.io/docs/concepts/containers/images/#updating-images) to use                                                                      |
| `pathling.volumes`                    | `[ ]`                           | A list of [volumes](https://kubernetes.io/docs/concepts/storage/volumes/) to mount in the pod                                                                               |
| `pathling.volumeMounts`               | `[ ]`                           | A list of [volume mounts](https://kubernetes.io/docs/concepts/storage/volumes/#using-volumes) to mount                                                                      |
| `pathling.serviceAccount`             | `~`                             | The [service account](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/) to assign to the pod                                             |
| `pathling.imagePullSecrets`           | `[ ]`                           | A list of [image pull secrets](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/) to use                                                |
| `pathling.tolerations`                | `[ ]`                           | A list of [tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/) to apply to the pod                                                  |
| `pathling.affinity`                   | `~`                             | [Affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity) to apply to the pod                                         |
| `pathling.securityContext`            | `~`                             | [Security context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/) for the pod                                                                  |
| `pathling.config`                     | `{ }`                           | A map of [configuration values](/docs/server/configuration.md) to pass to Pathling                                                                                          |
| `pathling.secretConfig`               | `{ }`                           | A map of secret configuration values to pass to Pathling, these values will be stored using [Kubernetes secrets](https://kubernetes.io/docs/concepts/configuration/secret/) |

## Example configuration[​](#example-configuration "Direct link to Example configuration")

Here are a few examples of how to configure the Pathling Helm chart for different deployment scenarios.

### Single node[​](#single-node "Direct link to Single node")

This configuration is suitable for a single node deployment of Pathling. In this scenario, all processing is performed on a single pod.

```
pathling:
  image: ghcr.io/aehrc/pathling:8
  resources:
    requests:
      cpu: 2
      memory: 4G
    limits:
      memory: 4G
    maxHeapSize: 3g
  volumes:
    - name: warehouse
      hostPath:
        path: /home/user/data/pathling
  volumeMounts:
    - name: warehouse
      mountPath: /usr/share/warehouse
      readOnly: false
  config:
    pathling.implementationDescription: My Pathling Server
    pathling.terminology.cache.maxEntries: 500000
    pathling.terminology.cache.overrideExpiry: "2592000"
    pathling.encoding.openTypes: string,code,decimal,Coding,Address
    logging.level.au.csiro.pathling: debug
```

### Cluster[​](#cluster "Direct link to Cluster")

This configuration is suitable for a cluster deployment of Pathling, using the [Spark Kubernetes cluster manager](https://spark.apache.org/docs/latest/running-on-kubernetes.html). In this scenario, the driver pod hosts an API but processing is performed on executor pods, which are spawned by the driver pod through calls to the Kubernetes API.

This configuration is suitable for the processing of larger datasets, or scenarios where it may be desirable to run a small driver pod and spawn executor pods on demand (at the cost of some latency).

```
pathling:
  image: ghcr.io/aehrc/pathling:8
  resources:
    requests:
      cpu: 1
      memory: 2G
    limits:
      memory: 2G
    maxHeapSize: 1500m
  volumes:
    - name: warehouse
      hostPath:
        path: /home/user/data/pathling
  volumeMounts:
    - name: warehouse
      mountPath: /usr/share/warehouse
      readOnly: false
  serviceAccount: spark-service-account
  config:
    pathling.implementationDescription: My Pathling Server
    pathling.terminology.cache.maxEntries: 500000
    pathling.terminology.cache.overrideExpiry: "2592000"
    pathling.encoding.openTypes: string,code,decimal,Coding,Address
    logging.level.au.csiro.pathling: debug
    spark.master: k8s://https://kubernetes.default.svc
    spark.kubernetes.namespace: pathling
    spark.kubernetes.executor.container.image: ghcr.io/aehrc/pathling:8
    spark.kubernetes.executor.volumes.hostPath.warehouse.options.path: /home/user/data/pathling
    spark.kubernetes.executor.volumes.hostPath.warehouse.mount.path: /usr/share/warehouse
    spark.kubernetes.executor.volumes.hostPath.warehouse.mount.readOnly: false
    spark.executor.instances: 3
    spark.executor.memory: 3G
    spark.kubernetes.executor.request.cores: 2
    spark.kubernetes.executor.limit.cores: 2
    spark.kubernetes.executor.request.memory: 4G
    spark.kubernetes.executor.limit.memory: 4G
```
