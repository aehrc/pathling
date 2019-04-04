/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * @author John Grimes
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class FhirLoaderConfiguration {

  @Nonnull
  private final List<String> resourcesToSave = new ArrayList<>();

  @Nonnull
  private String sparkMasterUrl;

  @Nonnull
  private String warehouseDirectory;

  @Nonnull
  private String metastoreUrl;

  @Nonnull
  private String metastoreUser;

  @Nonnull
  private String metastorePassword;

  private int loadPartitions;

  @Nonnull
  private String databaseName;

  @Nonnull
  private String executorMemory;

  public FhirLoaderConfiguration() {
    sparkMasterUrl = "spark://localhost:7077";
    warehouseDirectory = ".";
    metastoreUrl = "jdbc:postgresql://localhost/clinsight_metastore";
    metastoreUser = "clinsight";
    metastorePassword = "";
    databaseName = "clinsight";
    executorMemory = "1g";
    loadPartitions = 12;
  }

  @Nonnull
  public List<String> getResourcesToSave() {
    return resourcesToSave;
  }

  @Nonnull
  public String getSparkMasterUrl() {
    return sparkMasterUrl;
  }

  public void setSparkMasterUrl(@Nonnull String sparkMasterUrl) {
    this.sparkMasterUrl = sparkMasterUrl;
  }

  @Nonnull
  public String getWarehouseDirectory() {
    return warehouseDirectory;
  }

  public void setWarehouseDirectory(@Nonnull String warehouseDirectory) {
    this.warehouseDirectory = warehouseDirectory;
  }

  @Nonnull
  public String getMetastoreUrl() {
    return metastoreUrl;
  }

  public void setMetastoreUrl(@Nonnull String metastoreUrl) {
    this.metastoreUrl = metastoreUrl;
  }

  @Nonnull
  public String getMetastoreUser() {
    return metastoreUser;
  }

  public void setMetastoreUser(@Nonnull String metastoreUser) {
    this.metastoreUser = metastoreUser;
  }

  @Nonnull
  public String getMetastorePassword() {
    return metastorePassword;
  }

  public void setMetastorePassword(@Nonnull String metastorePassword) {
    this.metastorePassword = metastorePassword;
  }

  public int getLoadPartitions() {
    return loadPartitions;
  }

  public void setLoadPartitions(int loadPartitions) {
    this.loadPartitions = loadPartitions;
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

  @Override
  public String toString() {
    return "FhirLoaderConfiguration{" +
        "resourcesToSave=" + resourcesToSave +
        ", sparkMasterUrl='" + sparkMasterUrl + '\'' +
        ", warehouseDirectory='" + warehouseDirectory + '\'' +
        ", metastoreUrl='" + metastoreUrl + '\'' +
        ", metastoreUser='" + metastoreUser + '\'' +
        ", metastorePassword='" + metastorePassword + '\'' +
        ", loadPartitions=" + loadPartitions +
        ", databaseName='" + databaseName + '\'' +
        ", executorMemory='" + executorMemory + '\'' +
        '}';
  }

}

