/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class knows how to retrieve a Dataset representing all resources of a particular type, from
 * a specified database.
 *
 * @author John Grimes
 */
public class ResourceReader {

  private final SparkSession spark;
  private final String warehouseUrl;
  private final String databaseName;

  public ResourceReader(SparkSession spark, String warehouseUrl, String databaseName) {
    this.spark = spark;
    this.warehouseUrl = warehouseUrl;
    this.databaseName = databaseName;
  }

  public SparkSession getSpark() {
    return spark;
  }

  public String getWarehouseUrl() {
    return warehouseUrl;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Dataset<Row> read(String resourceName) {
    String tableUrl = warehouseUrl + "/" + databaseName + "/" + resourceName + ".parquet";
    return spark.read().parquet(tableUrl);
  }
}
