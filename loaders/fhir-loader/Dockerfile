FROM openjdk:8

COPY build/libs/fhir-loader-1.0-SNAPSHOT.jar /fhir-loader.jar
COPY build/dependencies /lib/

ENV CLINSIGHT_JSON_BUNDLES_DIRECTORY=/usr/share/fhir

CMD ["java", "-ea", "-jar", "/fhir-loader.jar"]