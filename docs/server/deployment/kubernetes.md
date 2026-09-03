# Kubernetes

[Kubernetes](https://kubernetes.io/) is an open-source system for automating deployment, scaling, and management of containerized applications. Support for deploying Pathling on Kubernetes is provided via a [Helm](https://helm.sh/) chart, which is available on [Artifact Hub](https://artifacthub.io/packages/helm/pathling/pathling).

The Helm chart includes the following features:

* Support for [startup, liveness and readiness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/) powered by the [Spring Boot Actuator](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html) endpoint
* Services for the FHIR API, Actuator management API, Spark UI, driver endpoint and block manager endpoint
* Support for the [Spark Kubernetes cluster manager](https://spark.apache.org/docs/latest/running-on-kubernetes.html), including a service account, role and role binding to allow it to manage executor pods and their on-demand scratch volumes. Executor pods are owned by the driver pod, so they are garbage collected when the driver pod is deleted
* A custom [JVM trust store](#custom-trust-store) for connecting to services that present certificates from a private certificate authority
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
| `pathling.truststore.enabled`         | `false`                         | Whether to mount a custom JVM trust store into the pod, see [Custom trust store](#custom-trust-store)                                                                       |
| `pathling.truststore.secretName`      | `pathling-truststore`           | The name of the Kubernetes secret that holds the trust store                                                                                                                |
| `pathling.truststore.key`             | `cacerts`                       | The data key within the secret that contains the trust store file                                                                                                           |
| `pathling.truststore.password`        | `changeit`                      | The trust store password                                                                                                                                                    |
| `pathling.truststore.type`            | `jks`                           | The trust store format, either `jks` or `pkcs12`                                                                                                                            |
| `pathling.truststore.mountPath`       | `/truststore`                   | The directory within the pod at which the trust store secret is mounted                                                                                                     |

Note that the chart only sets `JAVA_TOOL_OPTIONS` (and therefore `maxHeapSize`, `additionalJavaOptions` and the trust store options) when at least one entry is present in `pathling.config` or `pathling.secretConfig`.

## Custom trust store[​](#custom-trust-store "Direct link to Custom trust store")

If Pathling needs to connect to a terminology server, object store or other service that presents a certificate issued by a private certificate authority, the chart can mount a complete JVM trust store (JKS or PKCS12) and point the server's JVM at it.

The chart does not merge certificates into the default trust store. Build a store that contains every certificate authority the server must trust, for example by importing your internal root into a copy of the JDK `cacerts` file, and create a secret with a single data entry containing the store file:

```
kubectl create secret generic pathling-truststore \
  --from-file=cacerts=/path/to/cacerts
```

Then enable the trust store in the chart values:

```
pathling:
    truststore:
        enabled: true
        secretName: pathling-truststore
        key: cacerts
        password: changeit
        type: jks
```

The secret is mounted read-only at `pathling.truststore.mountPath` and `-Djavax.net.ssl.trustStore`, `-Djavax.net.ssl.trustStorePassword` and `-Djavax.net.ssl.trustStoreType` are appended to `JAVA_TOOL_OPTIONS`.

This applies to the driver pod only. In a [cluster](#cluster) deployment, executor pods are configured through Spark rather than the chart, so the same secret must be mounted and the same options passed via `pathling.config`:

```
pathling:
    config:
        spark.kubernetes.executor.secrets.pathling-truststore: /truststore
        spark.executorEnv.JAVA_TOOL_OPTIONS: -Djavax.net.ssl.trustStore=/truststore/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=jks
```

The chart's role grants the service account permission to read secrets so that both the driver and executor pods can mount the store.

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

### Cluster with SeaweedFS object storage[​](#cluster-with-seaweedfs-object-storage "Direct link to Cluster with SeaweedFS object storage")

A cluster deployment writes to the warehouse from every executor pod at once, with many small Parquet and Delta log files created, renamed and deleted during each import or query. Some Kubernetes storage classes cope poorly with this access pattern, even when they support `ReadWriteMany`. Network file systems such as NFS and CIFS can be slow under this many concurrent metadata operations, and some provisioners do not support `ReadWriteMany` at all, which prevents executor pods on different nodes from mounting the warehouse.

One way around this is to run [SeaweedFS](https://seaweedfs.com/) inside the cluster and point Pathling at its S3-compatible API. SeaweedFS holds its data on an ordinary `ReadWriteOnce` volume, so it can use whatever block storage class is available, and the driver and executor pods talk to it over HTTP rather than through a shared file system mount. The Pathling server image bundles the Hadoop S3A connector and the S3A "magic" committer, so no extra libraries are needed.

Install the [SeaweedFS Helm chart](https://github.com/seaweedfs/seaweedfs/tree/master/k8s/charts/seaweedfs) in the same namespace as Pathling. The all-in-one deployment runs the master, volume, filer and S3 gateway in a single pod, which is sufficient for a single-tenant Pathling installation. This example creates a `pathling-warehouse` bucket and enables S3 authentication:

```
global:
    seaweedfs:
        createClusterRole: false
master:
    enabled: false
volume:
    enabled: false
filer:
    enabled: false
allInOne:
    enabled: true
    replicas: 1
    updateStrategy:
        type: Recreate
    resources:
        requests:
            cpu: 2
            memory: 6Gi
        limits:
            cpu: 8
            memory: 24Gi
    data:
        type: persistentVolumeClaim
        storageClass: my-block-storage-class
        accessModes:
            - ReadWriteOnce
        size: 300Gi
    s3:
        enabled: true
        enableAuth: true
        createBuckets:
            - name: pathling-warehouse
s3:
    enableAuth: true
    credentials:
        admin:
            accessKey: <access key>
            secretKey: <secret key>
```

```
helm repo add seaweedfs https://seaweedfs.github.io/seaweedfs/helm
helm install pathling-seaweedfs seaweedfs/seaweedfs -f seaweedfs-values.yaml
```

The chart exposes the S3 gateway as a service named `<release>-seaweedfs-all-in-one` on port 8333.

Then configure Pathling to use the bucket as its warehouse. The warehouse volume and mounts from the [cluster](#cluster) example are no longer needed. The S3A settings are Spark configuration, so they are passed through `pathling.config` and are picked up by both the driver and the executors. The credentials go in `pathling.secretConfig` so that they are stored in a Kubernetes secret:

```
pathling:
    image: ghcr.io/aehrc/pathling:8
    serviceAccount: spark-service-account
    config:
        pathling.storage.warehouseUrl: s3a://pathling-warehouse
        spark.hadoop.fs.s3a.endpoint: http://pathling-seaweedfs-all-in-one:8333
        spark.hadoop.fs.s3a.path.style.access: "true"
        spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
        spark.hadoop.fs.s3a.committer.name: magic
        spark.sql.sources.commitProtocolClass: org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
        spark.sql.parquet.output.committer.class: org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
        spark.master: k8s://https://kubernetes.default.svc
        spark.kubernetes.namespace: pathling
        spark.kubernetes.executor.container.image: ghcr.io/aehrc/pathling:8
        spark.executor.instances: 3
        spark.executor.memory: 3G
        spark.kubernetes.executor.request.cores: 2
        spark.kubernetes.executor.limit.cores: 2
    secretConfig:
        spark.hadoop.fs.s3a.access.key: <access key>
        spark.hadoop.fs.s3a.secret.key: <secret key>
```

The [magic committer](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html) matters here. The default file output committer relies on renames to commit task output, and renames on an object store are copies followed by deletes, which are slow and not atomic. The magic committer writes each task's output as an S3 multipart upload and completes the upload at job commit, so there is no rename step. Incomplete uploads from failed tasks are left behind, so it is worth configuring SeaweedFS to clean them up periodically through the `master.config` value of the chart:

```
master:
    config: |-
        [master.maintenance]
        scripts = """
          lock
          s3.clean.uploads -timeAgo=24h
          unlock
        """
        sleep_minutes = 60
```

Because the warehouse is now behind an S3 API, it can be populated and inspected with any S3 client, for example by port-forwarding the gateway service and using the AWS CLI with `--endpoint-url`.
