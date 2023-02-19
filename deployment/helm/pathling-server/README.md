# Pathling Server Helm Chart

This repository contains a [Helm](https://helm.sh) chart that can install
an instance of [Pathling Server](https://pathling.csiro.au/docs/server) into a
Kubernetes cluster.

It deploys a single instance of Pathling, along with a front-side HTTP cache.

Here is a list of the supported values:

| Value                                      | Description                                                                                                                                                                                                                |
|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `pathling.image`                           | The Docker image to use for the Pathling server container. Defaults to `aehrc/pathling:6`.                                                                                                                                 |
| `pathling.cacheImage`                      | The Docker image to use for the cache container. Defaults to `aehrc/pathling-cache`.                                                                                                                                       |
| `pathling.ingress.hostName`                | The hostname that will be used to access the Ontoserver instance. This will be used in the Ingress resource.                                                                                                               |
| `pathling.ingress.class`                   | The ingress class that will be used for the Ingress resource. See [Kubernetes documentation - Ingress class](https://kubernetes.io/docs/concepts/services-networking/ingress/#ingress-class).                              |
| `pathling.ingress.annotations`             | A map of annotations that will be added to the Ingress resource.                                                                                                                                                           
| `pathling.resources.requests.cpu`          | The CPU request for the Pathling server container. See [Kubernetes documentation - Resource requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits).      |
| `pathling.resources.requests.memory`       | The memory request for the Pathling server container. See [Kubernetes documentation - Resource requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits).   |
| `pathling.resources.limits.memory`         | The memory limit for the Pathling server container. See [Kubernetes documentation - Resource requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits).     |
| `pathling.resources.cache.requests.cpu`    | The CPU request for the cache container. See [Kubernetes documentation - Resource requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits).                
| `pathling.resources.cache.requests.memory` | The memory request for the cache container. See [Kubernetes documentation - Resource requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits).             |
| `pathling.resources.cache.limits.memory`   | The memory limit for the cache container. See [Kubernetes documentation - Resource requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#requests-and-limits).               |
| `pathling.resources.maxHeapSize`           | The maximum JVM heap size for the Pathling server container.                                                                                                                                                               |
| `pathling.additionalJavaOptions`           | Additional JVM options to pass to the Pathling server container.                                                                                                                                                           |
| `pathling.deployment.strategy`             | The deployment strategy to use for the Pathling server deployment. See [Kubernetes documentation - Deployment strategies](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#strategy).                 |
| `pathling.deployment.imagePullPolicy`      | The image pull policy to use for the Pathling server deployment. See [Kubernetes documentation - Image pull policy](https://kubernetes.io/docs/concepts/containers/images/#updating-images).                               |
| `pathling.serviceAccount`                  | The service account to use for the Pathling server deployment. See [Kubernetes documentation - Service accounts](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/).                     |
| `pathling.imagePullSecrets`                | A list of image pull secrets to use for the Pathling server deployment. See [Kubernetes documentation - Image pull secrets](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).        |
| `pathling.tolerations.pathling`            | A list of tolerations to apply to the Pathling server deployment. See [Kubernetes documentation - Taints and tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/).                  |
| `pathling.tolerations.cache`               | A list of tolerations to apply to the cache deployment. See [Kubernetes documentation - Taints and tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/).                            |
| `pathling.affinity.pathling`               | The affinity to apply to the Pathling server deployment. See [Kubernetes documentation - Affinity and anti-affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity). |
| `pathling.affinity.cache`                  | The affinity to apply to the cache deployment. See [Kubernetes documentation - Affinity and anti-affinity](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity).           |
| `pathling.config`                          | A map of configuration properties to pass to the Pathling server. See [Pathling documentation - Server configuration](https://pathling.csiro.au/docs/server/configuration).                                                |
| `pathling.secretConfig`                    | A map of configuration properties to pass to the Pathling server. See [Pathling documentation - Server configuration](https://pathling.csiro.au/docs/server/configuration).                                                |

Copyright © 2023, Commonwealth Scientific and Industrial Research Organisation (
CSIRO) ABN 41 687 119 230. Licensed under the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
