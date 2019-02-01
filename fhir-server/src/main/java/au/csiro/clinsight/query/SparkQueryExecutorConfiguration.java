/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

/**
 * @author John Grimes
 */
public class SparkQueryExecutorConfiguration {

  private String sparkMasterUrl;
  private String warehouseDirectory;
  private String metastoreUrl;
  private String metastoreUser;
  private String metastorePassword;
  private String databaseName;
  private String executorMemory;
  private String terminologyServerUrl;

  public SparkQueryExecutorConfiguration() {
    sparkMasterUrl = "spark://localhost:7077";
    warehouseDirectory = ".";
    metastoreUrl = "jdbc:postgresql://localhost/clinsight_metastore";
    metastoreUser = "clinsight";
    metastorePassword = "";
    databaseName = "clinsight";
    executorMemory = "1g";
  }

  public String getSparkMasterUrl() {
    return sparkMasterUrl;
  }

  public void setSparkMasterUrl(String sparkMasterUrl) {
    this.sparkMasterUrl = sparkMasterUrl;
  }

  public String getWarehouseDirectory() {
    return warehouseDirectory;
  }

  public void setWarehouseDirectory(String warehouseDirectory) {
    this.warehouseDirectory = warehouseDirectory;
  }

  public String getMetastoreUrl() {
    return metastoreUrl;
  }

  public void setMetastoreUrl(String metastoreUrl) {
    this.metastoreUrl = metastoreUrl;
  }

  public String getMetastoreUser() {
    return metastoreUser;
  }

  public void setMetastoreUser(String metastoreUser) {
    this.metastoreUser = metastoreUser;
  }

  public String getMetastorePassword() {
    return metastorePassword;
  }

  public void setMetastorePassword(String metastorePassword) {
    this.metastorePassword = metastorePassword;
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

  @Override
  public String toString() {
    return "SparkQueryExecutorConfiguration{" +
        "sparkMasterUrl='" + sparkMasterUrl + '\'' +
        ", warehouseDirectory='" + warehouseDirectory + '\'' +
        ", metastoreUrl='" + metastoreUrl + '\'' +
        ", metastoreUser='" + metastoreUser + '\'' +
        ", metastorePassword='" + metastorePassword + '\'' +
        ", databaseName='" + databaseName + '\'' +
        ", executorMemory='" + executorMemory + '\'' +
        ", terminologyServerUrl='" + terminologyServerUrl + '\'' +
        '}';
  }
}
