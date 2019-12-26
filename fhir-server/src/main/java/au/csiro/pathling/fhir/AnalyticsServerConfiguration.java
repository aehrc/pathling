/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author John Grimes
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class AnalyticsServerConfiguration {

  /**
   * (OPTIONAL) Version of this API, as advertised within the CapabilityStatement.
   */
  @Nullable
  private String version;

  /**
   * (OPTIONAL) URL for the Apache Spark cluster this server should use.
   */
  @Nonnull
  private String sparkMasterUrl;

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
   * (OPTIONAL) URL of a FHIR terminology service to use in satisfying terminology queries.
   */
  @Nonnull
  private String terminologyServerUrl;

  /**
   * (OPTIONAL) Socket timeout for connections to the FHIR terminology service.
   */
  private int terminologySocketTimeout;

  /**
   * (OPTIONAL) Whether to run an explain ahead of each Spark SQL query.
   */
  private boolean explainQueries;

  /**
   * (OPTIONAL) Turns on verbose logging of the details of requests to the analytics server, and
   * requests to the terminology server.
   */
  private boolean verboseRequestLogging;

  /**
   * (OPTIONAL) Number of partitions to use when shuffling data for joins or aggregations.
   */
  private int shufflePartitions;

  /**
   * (OPTIONAL) Authentication information for reading and writing data using Amazon S3.
   */
  private String awsAccessKeyId;

  /**
   * (OPTIONAL) Authentication information for reading and writing data using Amazon S3.
   */
  private String awsSecretAccessKey;

  /**
   * (OPTIONAL) Allowed origins for the CORS configuration.
   */
  private List<String> corsAllowedOrigins;

  public AnalyticsServerConfiguration() {
    sparkMasterUrl = "local[*]";
    warehouseUrl = "file:///usr/share/warehouse";
    databaseName = "default";
    executorMemory = "1g";
    terminologyServerUrl = "https://r4.ontoserver.csiro.au/fhir";
    terminologySocketTimeout = 60000;
    explainQueries = false;
    verboseRequestLogging = false;
    shufflePartitions = 2;
    corsAllowedOrigins = Collections.singletonList("*");
  }

  @Nullable
  public String getVersion() {
    return version;
  }

  public void setVersion(@Nullable String version) {
    this.version = version;
  }

  @Nonnull
  public String getSparkMasterUrl() {
    return sparkMasterUrl;
  }

  public void setSparkMasterUrl(@Nonnull String sparkMasterUrl) {
    this.sparkMasterUrl = sparkMasterUrl;
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
  public String getTerminologyServerUrl() {
    return terminologyServerUrl;
  }

  public void setTerminologyServerUrl(@Nonnull String terminologyServerUrl) {
    this.terminologyServerUrl = terminologyServerUrl;
  }

  public int getTerminologySocketTimeout() {
    return terminologySocketTimeout;
  }

  public void setTerminologySocketTimeout(int terminologySocketTimeout) {
    this.terminologySocketTimeout = terminologySocketTimeout;
  }

  public boolean isExplainQueries() {
    return explainQueries;
  }

  public void setExplainQueries(boolean explainQueries) {
    this.explainQueries = explainQueries;
  }

  public boolean isVerboseRequestLogging() {
    return verboseRequestLogging;
  }

  public void setVerboseRequestLogging(boolean verboseRequestLogging) {
    this.verboseRequestLogging = verboseRequestLogging;
  }

  public int getShufflePartitions() {
    return shufflePartitions;
  }

  public void setShufflePartitions(int shufflePartitions) {
    this.shufflePartitions = shufflePartitions;
  }

  public String getAwsAccessKeyId() {
    return awsAccessKeyId;
  }

  public void setAwsAccessKeyId(String awsAccessKeyId) {
    this.awsAccessKeyId = awsAccessKeyId;
  }

  public String getAwsSecretAccessKey() {
    return awsSecretAccessKey;
  }

  public void setAwsSecretAccessKey(String awsSecretAccessKey) {
    this.awsSecretAccessKey = awsSecretAccessKey;
  }

  public List<String> getCorsAllowedOrigins() {
    return corsAllowedOrigins;
  }

  public void setCorsAllowedOrigins(List<String> corsAllowedOrigins) {
    this.corsAllowedOrigins = corsAllowedOrigins;
  }

  @Override
  public String toString() {
    return "AnalyticsServerConfiguration{" +
        "version='" + version + '\'' +
        ", sparkMasterUrl='" + sparkMasterUrl + '\'' +
        ", warehouseUrl='" + warehouseUrl + '\'' +
        ", databaseName='" + databaseName + '\'' +
        ", executorMemory='" + executorMemory + '\'' +
        ", terminologyServerUrl='" + terminologyServerUrl + '\'' +
        ", terminologySocketTimeout=" + terminologySocketTimeout +
        ", explainQueries=" + explainQueries +
        ", shufflePartitions=" + shufflePartitions +
        ", awsAccessKeyId='" + awsAccessKeyId + '\'' +
        ", corsAllowedOrigins=" + corsAllowedOrigins +
        '}';
  }

}
