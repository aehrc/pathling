FROM openjdk:8
ARG REVISION=master

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

WORKDIR /usr/src

# This line invalidates the cache of the following RUN command when the upstream repo is changed.
ADD https://api.github.com/repos/synthetichealth/synthea/git/refs/heads/$REVISION version.json

# Use a shallow clone of only the specified revision.
RUN git clone --depth 1 --branch $REVISION --single-branch https://github.com/synthetichealth/synthea.git && \
    cd synthea && \
    git checkout $REVISION && \
    ./gradlew --no-daemon build

WORKDIR /usr/src/synthea
ENTRYPOINT ["./run_synthea"]