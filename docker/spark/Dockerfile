FROM openjdk:8
ARG SPARK_VERSION
ARG SPARK_SCALA_VERSION
ARG HADOOP_VERSION
ARG SPARK_MIRROR
ARG HADOOP_MIRROR
ARG DEPENDENCY_PATH

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Dependencies for Spark scripts
RUN apt-get update && \
  apt-get install -y procps

# Download and extract Hadoop distribution
RUN cd /tmp && \
  wget -q ${HADOOP_MIRROR}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
  tar xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt --owner root --group root --no-same-owner && \
  rm hadoop-${HADOOP_VERSION}.tar.gz
RUN cd /opt && ln -s hadoop-${HADOOP_VERSION} hadoop

# Download and extract Spark distribution
RUN cd /tmp && \
  wget -q ${SPARK_MIRROR}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION}.tgz && \
  tar xzf spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION}.tgz -C /opt --owner root --group root --no-same-owner && \
  rm spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION}.tgz
RUN cd /opt && ln -s spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION} spark

# Copy FHIR server JAR to Spark classpath
ADD $DEPENDENCY_PATH /opt/spark/jars

COPY spark-env.sh /opt/spark/conf

# Run all Spark scripts in foreground
ENV SPARK_NO_DAEMONIZE=true
