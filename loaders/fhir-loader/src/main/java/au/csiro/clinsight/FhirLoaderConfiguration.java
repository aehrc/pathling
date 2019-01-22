/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import java.util.Arrays;

/**
 * @author John Grimes
 */
public class FhirLoaderConfiguration {

  private String sparkMasterUrl;
  private String warehouseDirectory;
  private String metastoreConnectionUrl;
  private String metastoreUser;
  private String metastorePassword;
  private int loadPartitions;
  private String databaseName;
  private String[] resourcesToSave;

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

  public String getMetastoreConnectionUrl() {
    return metastoreConnectionUrl;
  }

  public void setMetastoreConnectionUrl(String metastoreConnectionUrl) {
    this.metastoreConnectionUrl = metastoreConnectionUrl;
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

  public int getLoadPartitions() {
    return loadPartitions;
  }

  public void setLoadPartitions(int loadPartitions) {
    this.loadPartitions = loadPartitions;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String[] getResourcesToSave() {
    return resourcesToSave;
  }

  public void setResourcesToSave(String[] resourcesToSave) {
    this.resourcesToSave = resourcesToSave;
  }

  @Override
  public String toString() {
    return "FhirLoaderConfiguration{" +
        "sparkMasterUrl='" + sparkMasterUrl + '\'' +
        ", warehouseDirectory='" + warehouseDirectory + '\'' +
        ", metastoreConnectionUrl='" + metastoreConnectionUrl + '\'' +
        ", metastoreUser='" + metastoreUser + '\'' +
        ", metastorePassword='" + metastorePassword + '\'' +
        ", loadPartitions=" + loadPartitions +
        ", databaseName='" + databaseName + '\'' +
        ", resourcesToSave=" + Arrays.toString(resourcesToSave) +
        '}';
  }
}

