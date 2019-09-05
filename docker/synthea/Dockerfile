FROM openjdk:8
ARG REVISION=master

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

WORKDIR /usr/src
RUN git clone https://github.com/synthetichealth/synthea.git && \
    cd synthea && \
    git checkout $REVISION && \
    ./gradlew --no-daemon build

WORKDIR /usr/src/synthea
ENTRYPOINT ["./run_synthea"]