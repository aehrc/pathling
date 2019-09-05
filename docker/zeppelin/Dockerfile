FROM apache/zeppelin:0.8.1
ARG SPARK_VERSION
ARG SPARK_SCALA_VERSION
ARG SPARK_MIRROR
ARG FHIR_SERVER_JAR

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Download and extract Spark distribution
RUN cd /tmp && \
  wget -q ${SPARK_MIRROR}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION}.tgz && \
  tar xzf spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner && \
  rm spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION} spark

# Copy FHIR server JAR to Spark classpath
COPY ${FHIR_SERVER_JAR} /opt/spark/jars

# Install Python packages
RUN pip install matplotlib-venn

ENV SPARK_HOME /usr/local/spark
