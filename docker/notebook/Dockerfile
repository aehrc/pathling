FROM jupyter/all-spark-notebook
ARG FHIR_SERVER_JAR

LABEL copyright="Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved." \
  author="John Grimes <John.Grimes@csiro.au>"

# Copy FHIR server JAR to Spark classpath
COPY ${FHIR_SERVER_JAR} /opt/spark/jars

# Add any additional Python libraries.
RUN conda install --quiet -y pyarrow matplotlib matplotlib-venn plotnine && \
  conda clean -tipsy

# Start Jupyter Lab with authentication disabled
CMD ["start.sh", "jupyter", "lab", "--NotebookApp.token=''"]
