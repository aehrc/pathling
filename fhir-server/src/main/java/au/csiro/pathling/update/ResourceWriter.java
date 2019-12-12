/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.update;

import static au.csiro.pathling.utilities.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.utilities.PersistenceScheme.fileNameForResource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * This class knows how to persist a Dataset of resources within a specified database.
 *
 * @author John Grimes
 */
public class ResourceWriter {

  private final String warehouseUrl;
  private final String databaseName;

  public ResourceWriter(String warehouseUrl, String databaseName) {
    this.warehouseUrl = convertS3ToS3aUrl(warehouseUrl);
    this.databaseName = databaseName;
  }

  public String getWarehouseUrl() {
    return warehouseUrl;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void write(ResourceType resourceType, Dataset resources) {
    String tableUrl = warehouseUrl + "/" + databaseName + "/" + fileNameForResource(resourceType);
    resources.write().mode(SaveMode.Overwrite).parquet(tableUrl);
  }
}
