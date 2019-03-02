FROM docker-registry.it.csiro.au/clinsight/spark

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Master port
EXPOSE 7077/tcp

# REST API
EXPOSE 6066/tcp

# Master web UI
EXPOSE 8080/tcp

CMD bash -c "/opt/spark/sbin/start-master.sh" 2>&1

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 CMD jps -lm | grep org.apache.spark.deploy.master.Master
