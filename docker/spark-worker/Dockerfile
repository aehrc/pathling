ARG BASE_VERSION
FROM docker-registry.it.csiro.au/pathling/spark:$BASE_VERSION

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \ 
  author="John Grimes <John.Grimes@csiro.au>"

# Spark worker port
EXPOSE 49000/tcp

# Worker web UI
EXPOSE 8081/tcp

ENV SPARK_WORKER_PORT=49000

CMD bash -c "/opt/spark/sbin/start-slave.sh spark://foo:7077" 2>&1

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 CMD jps -lm | grep org.apache.spark.deploy.worker.Worker
