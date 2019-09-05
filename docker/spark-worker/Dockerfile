ARG BASE_VERSION
FROM docker-registry.it.csiro.au/clinsight/spark:$BASE_VERSION
ARG PYTHON_VERSION

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \ 
  author="John Grimes <John.Grimes@csiro.au>"

# Dependencies for Python installation
RUN apt-get -y update && \
  apt-get install -y build-essential zlib1g-dev

# Install Python 3.6.8, to enable Pyspark usage via notebook
RUN cd /tmp && \
  wget -q https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
  tar xzf Python-$PYTHON_VERSION.tgz -C /opt && \
  cd /opt/Python-$PYTHON_VERSION && \
  # ./configure --enable-optimizations && \
  ./configure && \
  make -j8 && \
  make altinstall

# Spark worker port
EXPOSE 49000/tcp

# Worker web UI
EXPOSE 8081/tcp

ENV SPARK_WORKER_PORT=49000

CMD bash -c "/opt/spark/sbin/start-slave.sh spark://foo:7077" 2>&1

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 CMD jps -lm | grep org.apache.spark.deploy.worker.Worker
