/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import ca.uhn.fhir.context.FhirContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class ExecutorConfiguration {

  /**
   * (REQUIRED) The Apache Spark session this server should use for import and query.
   */
  @Nonnull
  private final SparkSession sparkSession;

  /**
   * (REQUIRED) FHIR context for doing FHIR stuff locally.
   */
  @Nonnull
  private final FhirContext fhirContext;

  /**
   * (OPTIONAL) FHIR encoders for returning data as FHIR resources.
   */
  @Nullable
  private FhirEncoders fhirEncoders;

  /**
   * (OPTIONAL) Terminology client factory for doing terminology stuff on Spark workers.
   */
  @Nullable
  private final TerminologyClientFactory terminologyClientFactory;

  /**
   * (OPTIONAL) FHIR terminology service client to use in satisfying terminology queries locally.
   */
  @Nullable
  private final TerminologyClient terminologyClient;

  /**
   * (REQUIRED) Object for reading resources from the warehouse.
   */
  @Nonnull
  private final ResourceReader resourceReader;

  /**
   * (OPTIONAL) Version of this API, as advertised within the CapabilityStatement.
   */
  @Nullable
  private String version;

  /**
   * (OPTIONAL) URL for the location of warehouse tables.
   */
  @Nonnull
  private String warehouseUrl;

  /**
   * (OPTIONAL) Name of the database within the warehouse that this server should make available.
   */
  @Nonnull
  private String databaseName;

  /**
   * (OPTIONAL) Quantity of memory to make available to Spark executors.
   */
  @Nonnull
  private String executorMemory;


  /**
   * (OPTIONAL) Whether to run an explain ahead of each Spark SQL query.
   */
  private boolean explainQueries;

  /**
   * (OPTIONAL) Number of partitions to use when shuffling data for joins or aggregations.
   */
  private int shufflePartitions;

  /**
   * (OPTIONAL) Number of partitions to use when writing tables into the warehouse.
   */
  private int loadPartitions;

  public ExecutorConfiguration(@Nonnull SparkSession sparkSession,
      @Nonnull FhirContext fhirContext, @Nullable TerminologyClientFactory terminologyClientFactory,
      @Nullable TerminologyClient terminologyClient, @Nonnull ResourceReader resourceReader) {
    this.sparkSession = sparkSession;
    this.fhirContext = fhirContext;
    this.terminologyClientFactory = terminologyClientFactory;
    this.terminologyClient = terminologyClient;
    this.resourceReader = resourceReader;
    warehouseUrl = "file:///usr/share/warehouse";
    databaseName = "default";
    executorMemory = "1g";
    explainQueries = false;
    shufflePartitions = 36;
    loadPartitions = 12;
  }

  @Nonnull
  public SparkSession getSparkSession() {
    return sparkSession;
  }

  @Nonnull
  public FhirContext getFhirContext() {
    return fhirContext;
  }

  @Nullable
  public FhirEncoders getFhirEncoders() {
    return fhirEncoders;
  }

  public void setFhirEncoders(@Nullable FhirEncoders fhirEncoders) {
    this.fhirEncoders = fhirEncoders;
  }

  @Nullable
  public TerminologyClientFactory getTerminologyClientFactory() {
    return terminologyClientFactory;
  }

  @Nullable
  public TerminologyClient getTerminologyClient() {
    return terminologyClient;
  }

  @Nonnull
  public ResourceReader getResourceReader() {
    return resourceReader;
  }

  @Nullable
  public String getVersion() {
    return version;
  }

  public void setVersion(@Nullable String version) {
    this.version = version;
  }

  @Nonnull
  public String getWarehouseUrl() {
    return warehouseUrl;
  }

  public void setWarehouseUrl(@Nonnull String warehouseUrl) {
    this.warehouseUrl = warehouseUrl;
  }

  @Nonnull
  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(@Nonnull String databaseName) {
    this.databaseName = databaseName;
  }

  @Nonnull
  public String getExecutorMemory() {
    return executorMemory;
  }

  public void setExecutorMemory(@Nonnull String executorMemory) {
    this.executorMemory = executorMemory;
  }

  public boolean isExplainQueries() {
    return explainQueries;
  }

  public void setExplainQueries(boolean explainQueries) {
    this.explainQueries = explainQueries;
  }

  public int getShufflePartitions() {
    return shufflePartitions;
  }

  public void setShufflePartitions(int shufflePartitions) {
    this.shufflePartitions = shufflePartitions;
  }

  public int getLoadPartitions() {
    return loadPartitions;
  }

  public void setLoadPartitions(int loadPartitions) {
    this.loadPartitions = loadPartitions;
  }

  @Override
  public String toString() {
    return "ExecutorConfiguration{" +
        "terminologyClientFactory=" + terminologyClientFactory +
        ", resourceReader=" + resourceReader +
        ", version='" + version + '\'' +
        ", warehouseUrl='" + warehouseUrl + '\'' +
        ", databaseName='" + databaseName + '\'' +
        ", executorMemory='" + executorMemory + '\'' +
        ", explainQueries=" + explainQueries +
        ", shufflePartitions=" + shufflePartitions +
        ", loadPartitions=" + loadPartitions +
        '}';
  }

}
