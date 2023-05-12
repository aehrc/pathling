FROM jupyter/all-spark-notebook

USER root
COPY target/py-dist/pathling-*.whl /tmp

USER ${NB_UID}

RUN pip install --no-cache-dir /tmp/pathling-*.whl && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

# This caches the download of the dependencies specified earlier.
RUN source /usr/local/bin/before-notebook.d/spark-config.sh && \
    python -c "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate()"
