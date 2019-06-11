FROM apache/zeppelin:0.8.1
ARG SPARK_VERSION=2.3.3
ARG HADOOP_VERSION=2.7
ARG SPARK_MIRROR=http://mirror.intergrid.com.au/apache/spark

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Download and extract Spark distribution
RUN cd /tmp && \
  wget -q http://mirror.intergrid.com.au/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
  tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner && \
  rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

# Download PostgreSQL JAR
RUN cd /usr/local/spark/jars && \
  wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.5/postgresql-42.2.5.jar

# Install Python packages
RUN pip install matplotlib-venn

ENV SPARK_HOME /usr/local/spark
