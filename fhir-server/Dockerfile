FROM openjdk:8

COPY build/libs/fhir-server-1.0-SNAPSHOT.jar /fhir-server.jar
COPY build/dependencies /lib/

CMD ["java", "-ea", "-jar", "/fhir-server.jar"]