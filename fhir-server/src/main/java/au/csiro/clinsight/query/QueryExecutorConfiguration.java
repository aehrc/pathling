/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import au.csiro.clinsight.fhir.TerminologyClient;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class QueryExecutorConfiguration {

  private String sparkMasterUrl;
  private String warehouseUrl;
  private String databaseName;
  private String executorMemory;
  private String terminologyServerUrl;
  private boolean explainQueries;
  private TerminologyClient terminologyClient;
  private SparkSession sparkSession;

  public QueryExecutorConfiguration() {
    sparkMasterUrl = "spark://localhost:7077";
    databaseName = "clinsight";
    executorMemory = "1g";
    terminologyServerUrl = "http://localhost:8080/fhir";
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

  @Override
  public String toString() {
    return "QueryExecutorConfiguration{" +
        "sparkMasterUrl='" + sparkMasterUrl + '\'' +
        ", warehouseUrl='" + warehouseUrl + '\'' +
        ", databaseName='" + databaseName + '\'' +
        ", executorMemory='" + executorMemory + '\'' +
        ", terminologyServerUrl='" + terminologyServerUrl + '\'' +
        ", explainQueries=" + explainQueries +
        ", terminologyClient=" + terminologyClient +
        ", sparkSession=" + sparkSession +
        '}';
  }

}
