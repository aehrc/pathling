FROM openjdk:8

LABEL copyright="Copyright © 2018-2020, Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source Software Licence Agreement." \
  author="John Grimes <John.Grimes@csiro.au>"

# Dependencies for Hadoop native support
RUN apt-get update && apt-get install -y \
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
COPY $JAR_FILE /fhir-server.jar

RUN mkdir -p /usr/share/staging/test

ARG VERSION
ENV PATHLING_VERSION=$VERSION

CMD ["java", "-ea", "-jar", "/fhir-server.jar", "-Djava.library.path=/opt/hadoop/lib/native"]

HEALTHCHECK --interval=10s --timeout=1s --start-period=25s --retries=3 \
    CMD curl -v --fail http://localhost:8080/fhir/metadata || exit 1
    
EXPOSE 8080