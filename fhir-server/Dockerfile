FROM openjdk:8
ARG JAR_FILE
ARG DEPENDENCY_PATH
ARG VERSION

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

ADD $DEPENDENCY_PATH /usr/share/fhir-server/lib
COPY $JAR_FILE /fhir-server.jar
RUN mkdir -p /usr/share/staging/test

ENV CLINSIGHT_FHIR_SERVER_VERSION=$VERSION

CMD ["java", "-ea", "-cp", "/fhir-server.jar:/usr/share/fhir-server/lib/*", "au.csiro.clinsight.FhirServerContainer"]

HEALTHCHECK --interval=10s --timeout=1s --start-period=25s --retries=3 \
    CMD curl -v --fail http://localhost:8080/fhir/metadata || exit 1