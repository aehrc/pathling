---
sidebar_position: 4
---

# Spark configuration

If you are running your own Spark cluster, or using a Docker image (such
as [jupyter/all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook)), 
you will need to configure Pathling as a Spark package.

You can do this by adding the following to your `spark-defaults.conf` file:

```
spark.jars.packages au.csiro.pathling:library-api:[some version]
```

See the [Configuration](https://spark.apache.org/docs/latest/configuration.html)
page of the Spark documentation for more information about `spark.jars.packages`
and other related configuration options.

To create a Pathling notebook Docker image, your `Dockerfile` might look like
this:

```dockerfile
FROM jupyter/all-spark-notebook

USER root
RUN echo "spark.executor.userClassPathFirst true" >> /usr/local/spark/conf/spark-defaults.conf
RUN echo "spark.jars.packages au.csiro.pathling:library-api:[some version]" >> /usr/local/spark/conf/spark-defaults.conf

USER ${NB_UID}

RUN pip install --quiet --no-cache-dir pathling && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
```
