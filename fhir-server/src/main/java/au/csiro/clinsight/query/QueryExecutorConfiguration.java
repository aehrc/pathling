/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import au.csiro.clinsight.fhir.TerminologyClient;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class QueryExecutorConfiguration {

  /**
   * (OPTIONAL) Version of this API, as advertised within the CapabilityStatement.
   */
  @Nullable
  private String version;

  /**
   * (REQUIRED) The Apache Spark session this server should use for import and query.
   */
  @Nonnull
  private SparkSession sparkSession;

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
   * (REQUIRED) FHIR terminology service client to use in satisfying terminology queries.
   */
  @Nonnull
  private TerminologyClient terminologyClient;

  /**
   * (OPTIONAL) Object for reading resources from the warehouse.
   */
  @Nonnull
  private ResourceReader resourceReader;

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

  public QueryExecutorConfiguration(@Nonnull SparkSession sparkSession,
      @Nonnull TerminologyClient terminologyClient, @Nonnull ResourceReader resourceReader) {
    this.sparkSession = sparkSession;
    warehouseUrl = "file:///usr/share/warehouse";
    databaseName = "default";
    executorMemory = "1g";
    this.terminologyClient = terminologyClient;
    this.resourceReader = resourceReader;
    explainQueries = false;
    shufflePartitions = 36;
    loadPartitions = 12;
  }

  @Nullable
  public String getVersion() {
    return version;
  }

  public void setVersion(@Nullable String version) {
    this.version = version;
  }

  @Nonnull
  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public void setSparkSession(@Nonnull SparkSession sparkSession) {
    this.sparkSession = sparkSession;
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

  @Nonnull
  public TerminologyClient getTerminologyClient() {
    return terminologyClient;
  }

  public void setTerminologyClient(@Nonnull TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  @Nonnull
  public ResourceReader getResourceReader() {
    return resourceReader;
  }

  public void setResourceReader(@Nonnull ResourceReader resourceReader) {
    this.resourceReader = resourceReader;
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
    return "QueryExecutorConfiguration{" +
        "version='" + version + '\'' +
        ", warehouseUrl='" + warehouseUrl + '\'' +
        ", databaseName='" + databaseName + '\'' +
        ", executorMemory='" + executorMemory + '\'' +
        ", explainQueries=" + explainQueries +
        ", shufflePartitions=" + shufflePartitions +
        ", loadPartitions=" + loadPartitions +
        '}';
  }

}
