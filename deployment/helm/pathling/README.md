# Pathling Helm chart

Installs Pathling Server into a Kubernetes cluster. Includes the following
features:

- Support for startup, liveness and readiness probes powered by the Spring Boot
  Actuator endpoint
- Services for the FHIR API, Actuator management API, Spark UI, driver endpoint
  and block manager endpoint
- Support for the Spark Kubernetes cluster manager, including a service account,
  role and role binding to allow it to manage executor pods
- Customisation of resource requests and limits
- Configuration of volumes and volume mounts
- Image pull secrets for authenticated image registries
- Tolerations and affinity for control over pod scheduling
- Secret config for sensitive configuration values

## Installation

To install the chart, run the following commands:

```bash
helm repo add pathling https://pathling.csiro.au/helm
helm repo update
helm install pathling pathling/pathling
```

## Example configuration

### Single node

```yml
pathling:
  image: aehrc/pathling:6
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

### Cluster

```yml
pathling:
  image: aehrc/pathling:6
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
      spark.kubernetes.executor.container.image: aehrc/pathling:6
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

Pathling is copyright © 2018-2023, Commonwealth Scientific and Industrial
Research Organisation
(CSIRO) ABN 41 687 119 230. Licensed under
the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
