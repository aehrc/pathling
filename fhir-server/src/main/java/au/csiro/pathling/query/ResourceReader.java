/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import static au.csiro.pathling.utilities.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.utilities.PersistenceScheme.fileNameForResource;

import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to retrieve a Dataset representing all resources of a particular type, from
 * a specified database.
 *
 * @author John Grimes
 */
public class ResourceReader {

  private static final Logger logger = LoggerFactory.getLogger(ResourceReader.class);
  private final SparkSession spark;
  private final String warehouseUrl;
  private final String databaseName;
  private Set<ResourceType> availableResourceTypes = Collections
      .unmodifiableSet(EnumSet.noneOf(ResourceType.class));

  public ResourceReader(SparkSession spark, String warehouseUrl, String databaseName)
      throws IOException, URISyntaxException {
    this.spark = spark;
    this.warehouseUrl = convertS3ToS3aUrl(warehouseUrl);
    this.databaseName = databaseName;
    updateAvailableResourceTypes();
  }

  public void updateAvailableResourceTypes() throws IOException, URISyntaxException {
    logger.info("Getting available resource types");
    Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
    FileSystem warehouse = FileSystem.get(new URI(warehouseUrl), hadoopConfiguration);

    // Check that the database path exists.
    boolean exists;
    try {
      exists = warehouse.exists(new Path(warehouseUrl + "/" + databaseName));
    } catch (IOException e) {
      exists = false;
    }
    if (!exists) {
      availableResourceTypes = EnumSet.noneOf(ResourceType.class);
      return;
    }

    // Find all the Parquet files within the warehouse and use them to create a set of resource
    // types.
    FileStatus[] fileStatuses = warehouse.listStatus(new Path(warehouseUrl + "/" + databaseName));
    availableResourceTypes = Arrays.stream(fileStatuses)
        .map(fileStatus -> {
          String code = fileStatus.getPath().getName().replace(".parquet", "");
          return ResourceType.fromCode(code);
        })
        .collect(Collectors.toSet());
    availableResourceTypes = Collections.unmodifiableSet(availableResourceTypes);
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

  public Set<ResourceType> getAvailableResourceTypes() {
    return availableResourceTypes;
  }

  public Dataset<Row> read(ResourceType resourceType) {
    if (!availableResourceTypes.contains(resourceType)) {
      throw new ResourceNotFoundException(
          "Requested resource type not available within selected database: " + resourceType
              .toCode());
    }
    String tableUrl = warehouseUrl + "/" + databaseName + "/" + fileNameForResource(resourceType);
    Dataset<Row> resources = spark.read().parquet(tableUrl);
    resources.cache();
    return resources;
  }

  @Override
  public String toString() {
    return "ResourceReader{" +
        "warehouseUrl='" + warehouseUrl + '\'' +
        ", databaseName='" + databaseName + '\'' +
        ", availableResourceTypes=" + availableResourceTypes +
        '}';
  }

}
