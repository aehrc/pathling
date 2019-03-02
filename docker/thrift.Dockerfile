FROM docker-registry.it.csiro.au/clinsight/spark

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# JDBC/ODBC port
EXPOSE 10000/tcp

CMD bash -c "/opt/spark/sbin/start-thriftserver.sh" 2>&1

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 CMD jps -lm | grep org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
