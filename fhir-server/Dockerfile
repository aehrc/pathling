FROM openjdk:8
ARG version

COPY build/libs/fhir-server-1.0-SNAPSHOT.jar /fhir-server.jar
COPY build/dependencies /lib/

ENV CLINSIGHT_FHIR_SERVER_VERSION=$version

CMD ["java", "-ea", "-jar", "/fhir-server.jar"]