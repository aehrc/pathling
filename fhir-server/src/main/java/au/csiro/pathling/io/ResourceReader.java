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
import au.csiro.pathling.security.PathlingAuthority.AccessType;
import au.csiro.pathling.security.ResourceAccess;
import io.delta.tables.DeltaTable;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
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
  protected final ResourceWriter resourceWriter;

  @Nonnull
  private final String warehouseUrl;

  @Nonnull
  private final String databaseName;

  /**
   * @param configuration a {@link Configuration} object which controls the behaviour of the reader
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param resourceWriter {@link ResourceWriter} for creating and writing empty datasets
   */
  public ResourceReader(@Nonnull final Configuration configuration,
      @Nonnull final SparkSession spark, @Nonnull final ResourceWriter resourceWriter) {
    this.configuration = configuration;
    this.spark = spark;
    this.warehouseUrl = convertS3ToS3aUrl(configuration.getStorage().getWarehouseUrl());
    this.databaseName = configuration.getStorage().getDatabaseName();
    this.resourceWriter = resourceWriter;
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
    return attemptDeltaLoad(resourceType)
        .map(DeltaTable::toDF)
        // If there is no existing table, we return an empty table with the right shape.
        .orElseGet(() -> resourceWriter.createEmptyDataset(resourceType));
  }

  /**
   * Reads a set of resources of a particular type from the warehouse location.
   *
   * @param resourceType the desired {@link ResourceType}
   * @return a {@link DeltaTable} containing the raw resource, i.e. NOT wrapped in a value column
   */
  @Nonnull
  DeltaTable readDelta(@Nonnull final ResourceType resourceType) {
    return attemptDeltaLoad(resourceType).orElseGet(() -> {
      // If a Delta access is attempted upon a resource which does not yet exist, we create an empty 
      // table. This is because Delta operations cannot be done in absence of a persisted table.
      final String tableUrl = resourceWriter.writeEmpty(resourceType);
      return getDeltaTable(resourceType, tableUrl);
    });
  }

  /**
   * Attempts to load a Delta table for the given resource type. Tables may not exist if they have
   * not previously been the subject of write operations.
   */
  private Optional<DeltaTable> attemptDeltaLoad(@Nonnull final ResourceType resourceType) {
    final String tableUrl = getTableUrl(warehouseUrl, databaseName, resourceType);
    return DeltaTable.isDeltaTable(spark, tableUrl)
           ? Optional.of(getDeltaTable(resourceType, tableUrl))
           : Optional.empty();
  }

  /**
   * Loads a Delta table that we already know exists.
   */
  @Nonnull
  private DeltaTable getDeltaTable(final @Nonnull ResourceType resourceType,
      final String tableUrl) {
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