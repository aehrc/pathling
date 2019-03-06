FROM openjdk:8
ARG SPARK_VERSION=2.3.3
ARG HADOOP_VERSION=2.7
ARG SPARK_MIRROR=http://mirror.intergrid.com.au/apache/spark

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Dependencies for Spark scripts
RUN apt-get update && \
  apt-get install -y procps

# Download and extract Spark distribution
RUN cd /tmp && \
  wget -q ${SPARK_MIRROR}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
  tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt --owner root --group root --no-same-owner && \
  rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /opt && ln -s spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

# Download PostgreSQL JAR
RUN cd /opt/spark/jars && \
  wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.5/postgresql-42.2.5.jar

# Run all Spark scripts in foreground
ENV SPARK_NO_DAEMONIZE=true
