FROM jupyter/all-spark-notebook

USER root
RUN echo "spark.jars.packages au.csiro.pathling:library-runtime:7.0.0" >> /usr/local/spark/conf/spark-defaults.conf

USER ${NB_UID}

RUN pip install --quiet --no-cache-dir pathling && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

# This caches the download of the dependencies specified earlier.
RUN source /usr/local/bin/before-notebook.d/spark-config.sh && \
    python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate()"
