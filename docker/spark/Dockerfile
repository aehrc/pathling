FROM openjdk:8
ARG SPARK_VERSION
ARG SPARK_SCALA_VERSION
ARG HADOOP_VERSION
ARG SPARK_MIRROR
ARG HADOOP_MIRROR

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Dependencies for Spark scripts and Hadoop native support
RUN apt-get update && apt-get install -y \
  libsnappy-dev \
  libssl-dev \
  libzstd-dev \
  procps

# Download and extract Hadoop distribution
RUN cd /tmp && \
  wget -q ${HADOOP_MIRROR}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
  tar xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt --owner root --group root --no-same-owner && \
  rm hadoop-${HADOOP_VERSION}.tar.gz
RUN cd /opt && ln -s hadoop-${HADOOP_VERSION} hadoop

# Download and extract Spark distribution
ENV SPARK_PREFIX="spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION}"

RUN cd /tmp && \
  wget -q ${SPARK_MIRROR}/spark-${SPARK_VERSION}/${SPARK_PREFIX%-scala-2.11}.tgz && \
  tar xzf ${SPARK_PREFIX%-scala-2.11}.tgz -C /opt --owner root --group root --no-same-owner && \
  rm ${SPARK_PREFIX%-scala-2.11}.tgz

RUN cd /opt && ln -s ${SPARK_PREFIX%-scala-2.11} spark

COPY spark-env.sh /opt/spark/conf

ENV HADOOP_HOME /opt/hadoop
ENV SPARK_HOME /opt/spark
# Run all Spark scripts in foreground
ENV SPARK_NO_DAEMONIZE=true

# Copy FHIR server JAR to Spark classpath
ARG DEPENDENCY_PATH
ADD $DEPENDENCY_PATH /opt/spark/jars
