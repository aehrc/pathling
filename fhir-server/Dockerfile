FROM openjdk:8
ARG JAR_FILE
ARG DEPENDENCY_PATH
ARG VERSION
ARG HADOOP_MIRROR
ARG HADOOP_VERSION

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Dependencies for Hadoop native support
RUN apt-get update && apt-get install -y \
  libsnappy-dev \
  libssl-dev \
  libzstd-dev

# Download and extract Hadoop distribution
RUN cd /tmp && \
  wget -q ${HADOOP_MIRROR}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
  tar xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt --owner root --group root --no-same-owner && \
  rm hadoop-${HADOOP_VERSION}.tar.gz
RUN cd /opt && ln -s hadoop-${HADOOP_VERSION} hadoop

ADD $DEPENDENCY_PATH /usr/share/fhir-server/lib
COPY $JAR_FILE /fhir-server.jar
RUN mkdir -p /usr/share/staging/test

ENV CLINSIGHT_FHIR_SERVER_VERSION=$VERSION

CMD ["java", "-ea", "-cp", "/fhir-server.jar:/usr/share/fhir-server/lib/*", \
  "-Djava.library.path=/opt/hadoop/lib/native", "au.csiro.clinsight.FhirServerContainer"]

HEALTHCHECK --interval=10s --timeout=1s --start-period=25s --retries=3 \
    CMD curl -v --fail http://localhost:8080/fhir/metadata || exit 1