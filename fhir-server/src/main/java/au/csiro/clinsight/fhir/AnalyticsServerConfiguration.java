/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class AnalyticsServerConfiguration {

  private String version;
  private String sparkMasterUrl;
  private String warehouseUrl;
  private String databaseName;
  private String executorMemory;
  private String terminologyServerUrl;
  private boolean explainQueries;
  private TerminologyClient terminologyClient;
  private SparkSession sparkSession;
  private int shufflePartitions;
  private int loadPartitions;

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getSparkMasterUrl() {
    return sparkMasterUrl;
  }

  public void setSparkMasterUrl(String sparkMasterUrl) {
    this.sparkMasterUrl = sparkMasterUrl;
  }

  public String getWarehouseUrl() {
    return warehouseUrl;
  }

  public void setWarehouseUrl(String warehouseUrl) {
    this.warehouseUrl = warehouseUrl;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getExecutorMemory() {
    return executorMemory;
  }

  public void setExecutorMemory(String executorMemory) {
    this.executorMemory = executorMemory;
  }

  public String getTerminologyServerUrl() {
    return terminologyServerUrl;
  }

  public void setTerminologyServerUrl(String terminologyServerUrl) {
    this.terminologyServerUrl = terminologyServerUrl;
  }

  public boolean getExplainQueries() {
    return explainQueries;
  }

  public void setExplainQueries(boolean explainQueries) {
    this.explainQueries = explainQueries;
  }

  public TerminologyClient getTerminologyClient() {
    return terminologyClient;
  }

  public void setTerminologyClient(TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public void setSparkSession(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
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
    return "AnalyticsServerConfiguration{" +
        "version='" + version + '\'' +
        ", sparkMasterUrl='" + sparkMasterUrl + '\'' +
        ", warehouseUrl='" + warehouseUrl + '\'' +
        ", databaseName='" + databaseName + '\'' +
        ", executorMemory='" + executorMemory + '\'' +
        ", terminologyServerUrl='" + terminologyServerUrl + '\'' +
        ", explainQueries=" + explainQueries +
        ", terminologyClient=" + terminologyClient +
        ", sparkSession=" + sparkSession +
        ", shufflePartitions=" + shufflePartitions +
        ", loadPartitions=" + loadPartitions +
        '}';
  }

}
