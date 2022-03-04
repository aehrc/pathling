/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.io.PersistenceScheme.getTableUrl;
import static org.apache.spark.sql.functions.asc;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.security.PathlingAuthority.AccessType;
import au.csiro.pathling.security.ResourceAccess;
import io.delta.tables.DeltaTable;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class knows how to persist a Dataset of resources within a specified database.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
@Slf4j
public class ResourceWriter {

  @Nonnull
  private final String warehouseUrl;

  @Nonnull
  private final String databaseName;

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  /**
   * @param configuration A {@link Configuration} object which controls the behaviour of the writer
   * @param spark a {@link SparkSession} for creating empty datasets
   * @param fhirEncoders a {@link FhirEncoders} object for creating empty datasets
   */
  public ResourceWriter(@Nonnull final Configuration configuration,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders) {
    this.warehouseUrl = convertS3ToS3aUrl(configuration.getStorage().getWarehouseUrl());
    this.databaseName = configuration.getStorage().getDatabaseName();
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
  }

  /**
   * Overwrites the resources for a particular type with the contents of the supplied {@link
   * Dataset}.
   *
   * @param resourceType the type of the resource to write
   * @param resources the {@link Dataset} containing the resource data
   */
  @ResourceAccess(AccessType.WRITE)
  public void write(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> resources) {
    write(resourceType, resources, SaveMode.Overwrite);
  }

  @ResourceAccess(AccessType.WRITE)
  public void append(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> resources) {
    final String tableUrl = getTableUrl(warehouseUrl, databaseName, resourceType);
    log.debug("Appending to dataset: {}", resourceType.toCode());
    resources
        .write()
        .mode(SaveMode.Append)
        .format("delta")
        .save(tableUrl);
  }

  @ResourceAccess(AccessType.WRITE)
  public void update(@Nonnull final ResourceType resourceType,
      @Nonnull final ResourceReader resourceReader, @Nonnull final Dataset<Row> resources) {
    final DeltaTable original = resourceReader.readDelta(resourceType);
    log.debug("Writing updates to dataset: {}", resourceType.toCode());
    original
        .as("original")
        .merge(resources.as("updates"), "original.id = updates.id")
        .whenMatched()
        .updateAll()
        .execute();
  }

  /**
   * Overwrites the resources for a particular type with the contents of the supplied {@link
   * Dataset}.
   *
   * @param resourceType the type of the resource to write
   * @param resources the {@link Dataset} containing the resource data
   * @param saveMode the {@link SaveMode} to use when writing the data
   */
  void write(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> resources, @Nonnull final SaveMode saveMode) {
    final String tableUrl = getTableUrl(warehouseUrl, databaseName, resourceType);
    log.debug("Writing resources to: {}", tableUrl);
    // We order the resources here to reduce the amount of sorting necessary at query time.
    resources.orderBy(asc("id"))
        .write()
        // By default, Delta throws an error if the incoming schema is different to the existing 
        // one. For the purposes of this method, we want to be able to rewrite the schema in cases 
        // where it has changed, e.g. a version upgrade or a configuration change.
        // See: https://docs.delta.io/latest/delta-batch.html#replace-table-schema
        .option("overwriteSchema", "true")
        .mode(saveMode)
        .format("delta")
        .save(tableUrl);
  }

  @Nonnull
  String writeEmpty(@Nonnull final ResourceType resourceType) {
    final Dataset<Row> dataset = createEmptyDataset(resourceType);
    log.debug("Writing empty dataset: {}", resourceType.toCode());
    write(resourceType, dataset, SaveMode.ErrorIfExists);
    return getTableUrl(warehouseUrl, databaseName, resourceType);
  }

  @Nonnull
  Dataset<Row> createEmptyDataset(final @Nonnull ResourceType resourceType) {
    return QueryHelpers.createEmptyDataset(spark, fhirEncoders, resourceType);
  }

}
