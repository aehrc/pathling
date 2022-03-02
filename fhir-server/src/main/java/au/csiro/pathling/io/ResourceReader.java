/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.io.PersistenceScheme.getTableUrl;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.security.PathlingAuthority.AccessType;
import au.csiro.pathling.security.ResourceAccess;
import io.delta.tables.DeltaTable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class knows how to retrieve a Dataset representing all resources of a particular type, from
 * a specified database.
 *
 * @author John Grimes
 */
@Component
@Profile("core & !ga4gh")
@Slf4j
public class ResourceReader {

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  protected final SparkSession spark;

  @Nonnull
  private final String warehouseUrl;

  @Nonnull
  private final String databaseName;

  @Nonnull
  private Set<ResourceType> availableResourceTypes = Collections
      .unmodifiableSet(EnumSet.noneOf(ResourceType.class));

  /**
   * @param configuration a {@link Configuration} object which controls the behaviour of the reader
   * @param spark a {@link SparkSession} for interacting with Spark
   */
  public ResourceReader(@Nonnull final Configuration configuration,
      @Nonnull final SparkSession spark) {
    this.configuration = configuration;
    this.spark = spark;
    this.warehouseUrl = convertS3ToS3aUrl(configuration.getStorage().getWarehouseUrl());
    this.databaseName = configuration.getStorage().getDatabaseName();
    updateAvailableResourceTypes();
  }

  /**
   * Checks the warehouse location and updates the available resource types.
   */
  public void updateAvailableResourceTypes() {
    log.debug("Getting available resource types");
    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    checkNotNull(hadoopConfiguration);
    @Nullable final FileSystem warehouse;
    try {
      warehouse = FileSystem.get(new URI(warehouseUrl), hadoopConfiguration);
    } catch (final IOException e) {
      throw new RuntimeException("Problem accessing warehouse location: " + warehouseUrl, e);
    } catch (final URISyntaxException e) {
      throw new RuntimeException("Problem parsing warehouse URL: " + warehouseUrl, e);
    }
    checkNotNull(warehouse);

    // Check that the database path exists.
    final String databasePath = warehouseUrl + "/" + databaseName;
    try {
      warehouse.exists(new Path(databasePath));
    } catch (final IOException e) {
      throw new RuntimeException(
          "Problem accessing database path within warehouse location: " + databaseName, e);
    }

    // Find all the Parquet files within the warehouse and use them to create a set of resource
    // types.
    @Nullable final FileStatus[] fileStatuses;
    try {
      fileStatuses = warehouse.listStatus(new Path(databasePath));
    } catch (final IOException e) {
      throw new RuntimeException(
          "Problem listing file status at database path: " + databasePath, e);
    }
    checkNotNull(fileStatuses);

    availableResourceTypes = Arrays.stream(fileStatuses)
        // Get the filename of each item in the directory listing.
        .map(fileStatus -> {
          @Nullable final Path path = fileStatus.getPath();
          checkNotNull(path);
          @Nullable final String name = path.getName();
          checkNotNull(name);
          return name;
        })
        // Filter out any file names that don't match the pattern.
        .filter(fileName -> fileName.matches("^[^.]+\\.parquet$"))
        // Grab the resource code indicated by each matching file name.
        .map(fileName -> {
          final String code = fileName.replace(".parquet", "");
          return ResourceType.fromCode(code);
        })
        .collect(Collectors.toSet());
    availableResourceTypes = Collections.unmodifiableSet(availableResourceTypes);
    log.info("Available resources: {}", availableResourceTypes);
  }

  /**
   * @return The set of resource types currently available for reading.
   */
  @Nonnull
  public Set<ResourceType> getAvailableResourceTypes() {
    return availableResourceTypes;
  }

  /**
   * Reads a set of resources of a particular type from the warehouse location.
   *
   * @param resourceType the desired {@link ResourceType}
   * @return a {@link Dataset} containing the raw resource, i.e. NOT wrapped in a value column
   */
  @ResourceAccess(AccessType.READ)
  @Nonnull
  public Dataset<Row> read(@Nonnull final ResourceType resourceType) {
    return loadAsDelta(resourceType).toDF();
  }

  /**
   * Reads a set of resources of a particular type from the warehouse location.
   *
   * @param resourceType the desired {@link ResourceType}
   * @return a {@link DeltaTable} containing the raw resource, i.e. NOT wrapped in a value column
   */
  @ResourceAccess(AccessType.READ)
  @Nonnull
  public DeltaTable readDelta(@Nonnull final ResourceType resourceType) {
    return loadAsDelta(resourceType);
  }

  private DeltaTable loadAsDelta(@Nonnull final ResourceType resourceType) {
    if (!getAvailableResourceTypes().contains(resourceType)) {
      throw new ResourceNotFoundError(
          "Requested resource type not available within selected database: " + resourceType
              .toCode());
    }
    final String tableUrl = getTableUrl(warehouseUrl, databaseName, resourceType);

    log.info("Loading resource {} from: {}", resourceType.toCode(), tableUrl);
    @Nullable final DeltaTable resources = DeltaTable.forPath(spark, tableUrl);
    checkNotNull(resources);

    if (configuration.getSpark().getCacheDatasets()) {
      // Cache the raw resource data.
      log.debug("Caching resource dataset: {}", resourceType.toCode());
      resources.toDF().cache();
    }

    return resources;
  }

}