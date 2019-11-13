FROM apache/zeppelin:0.8.1
ARG SPARK_VERSION
ARG SPARK_SCALA_VERSION
ARG SPARK_MIRROR
ARG FHIR_SERVER_JAR

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Download and extract Spark distribution
ENV SPARK_PREFIX="spark-${SPARK_VERSION}-bin-without-hadoop-scala-${SPARK_SCALA_VERSION}"

RUN cd /tmp && \
  wget -q ${SPARK_MIRROR}/spark-${SPARK_VERSION}/${SPARK_PREFIX%-scala-2.11}.tgz && \
  tar xzf ${SPARK_PREFIX%-scala-2.11}.tgz -C /opt --owner root --group root --no-same-owner && \
  rm ${SPARK_PREFIX%-scala-2.11}.tgz

RUN cd /opt && ln -s ${SPARK_PREFIX%-scala-2.11} spark

# Copy FHIR server JAR to Spark classpath
COPY ${FHIR_SERVER_JAR} /opt/spark/jars

# Install Python packages
RUN pip install matplotlib-venn

ENV SPARK_HOME /usr/local/spark
