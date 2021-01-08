FROM openjdk:11-jre-slim

LABEL copyright="Copyright © 2018-2021, Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source Software Licence Agreement." \
  author="John Grimes <John.Grimes@csiro.au>"

RUN apt-get update && apt-get install -y \
  curl wget \
  # Dependencies for Hadoop native support
  libsnappy-dev \
  libssl-dev \
  libzstd-dev

# Download and extract Hadoop distribution
ARG HADOOP_MIRROR
ARG HADOOP_VERSION
RUN cd /tmp && \
  wget -q ${HADOOP_MIRROR}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
  tar xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt --owner root --group root --no-same-owner && \
  rm hadoop-${HADOOP_VERSION}.tar.gz
RUN cd /opt && ln -s hadoop-${HADOOP_VERSION} hadoop

ARG JAR_FILE
COPY $JAR_FILE /pathling.jar

RUN mkdir -p /usr/share/staging/test

ARG VERSION
ENV PATHLING_VERSION=$VERSION
ENV JAVA_OPTS=-Xmx2g

COPY run.sh /
CMD ["bash", "/run.sh"]

HEALTHCHECK --interval=10s --timeout=1s --start-period=25s --retries=3 \
    CMD curl -v --fail http://localhost:8080/fhir/metadata || exit 1

# This is the port for the main FHIR server.
EXPOSE 8080

# This is the port for the Spark web UI.
EXPOSE 4040