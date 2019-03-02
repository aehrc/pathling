FROM jupyter/scipy-notebook:7f1482f5a136
ARG SPARK_VERSION=2.3.3
ARG HADOOP_VERSION=2.7
ARG SPARK_MIRROR=http://mirror.intergrid.com.au/apache/spark

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

USER root

# Install JRE
RUN apt-get -y update && \
  apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
  rm -rf /var/lib/apt/lists/*

# Download and extract Spark distribution
RUN cd /tmp && \
  wget -q http://mirror.intergrid.com.au/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
  tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner && \
  rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

ENV SPARK_HOME /usr/local/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip
ENV SPARK_OPTS --driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info

# Configures the Python executable used on worker instances, must be exactly the same version as PYSPARK_DRIVER_PYTHON
ENV PYSPARK_PYTHON /usr/local/bin/python3.6

# Configures the Python executable used on the notebook instance, this is 3.6.8
ENV PYSPARK_DRIVER_PYTHON /opt/conda/bin/python

USER $NB_UID

RUN conda install --quiet -y 'pyarrow' && \
  conda clean -tipsy && \
  fix-permissions $CONDA_DIR && \
  fix-permissions /home/$NB_USER

# Application web UI
EXPOSE 4040/tcp

# Start Jupyter Lab with authentication disabled
CMD ["start.sh", "jupyter", "lab", "--NotebookApp.token=''"]
