FROM jupyter/all-spark-notebook

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Add any additional Python libraries.
RUN conda install --quiet -y pyarrow matplotlib matplotlib-venn plotnine && \
  conda clean -tipsy

# Start Jupyter Lab with authentication disabled
CMD ["start.sh", "jupyter", "lab", "--NotebookApp.token=''"]

# Copy FHIR server JAR to Spark classpath
ARG FHIR_SERVER_JAR
COPY ${FHIR_SERVER_JAR} /opt/spark/jars
