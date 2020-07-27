#!/usr/bin/env bash

java -ea -cp /fhir-server.jar:/usr/share/fhir-server/lib/* -Djava.library.path=/opt/hadoop/lib/native $JAVA_OPTS au.csiro.pathling.FhirServerContainer