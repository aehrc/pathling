/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.QueryHelpers.createEmptyDataset;
import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.io.PersistenceScheme.getTableUrl;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.asc;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.security.PathlingAuthority.AccessType;
import au.csiro.pathling.security.ResourceAccess;
import io.delta.tables.DeltaTable;
import io.delta.tables.DeltaTableBuilder;
import java.io.File;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.FileSystemUtils;

/**
 * Used for reading and writing resource data in persistent storage.
 *
 * @author John Grimes
 */
@Component
@Profile("(core | import) & !ga4gh")
@Slf4j
public class Database {

  @Nonnull
  private final String warehouseUrl;

  @Nonnull
  private final String databaseName;

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  protected final SparkSession spark;

  @Nonnull
  protected final FhirEncoders fhirEncoders;

  /**
   * @param configuration a {@link Configuration} object which controls the behaviour of the reader
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   */
  public Database(@Nonnull final Configuration configuration,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders) {
    this.configuration = configuration;
    this.spark = spark;
    this.warehouseUrl = convertS3ToS3aUrl(configuration.getStorage().getWarehouseUrl());
    this.databaseName = configuration.getStorage().getDatabaseName();
    this.fhirEncoders = fhirEncoders;
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
        .orElseGet(() -> createEmptyDataset(spark, fhirEncoders, resourceType));
  }

  /**
   * Overwrites the resources for a particular type with the contents of the supplied {@link
   * Dataset}.
   *
   * @param resourceType the type of the resource to write
   * @param resources the {@link Dataset} containing the resource data
   */
  @ResourceAccess(AccessType.WRITE)
  public void overwrite(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> resources) {
    write(resourceType, resources, SaveMode.Overwrite);
  }

  /**
   * Creates or updates a single resource of the specified type by matching on ID.
   *
   * @param resourceType the type of resource to write
   * @param resource the new or updated resource
   */
  @ResourceAccess(AccessType.WRITE)
  public void merge(@Nonnull final ResourceType resourceType,
      @Nonnull final IBaseResource resource) {
    merge(resourceType, List.of(resource));
  }

  /**
   * Creates or updates resources of the specified type by matching on ID.
   *
   * @param resourceType the type of resource to write
   * @param resources a list containing the new or updated resource data
   */
  @ResourceAccess(AccessType.WRITE)
  public void merge(@Nonnull final ResourceType resourceType,
      @Nonnull final List<IBaseResource> resources) {
    final Encoder<IBaseResource> encoder = fhirEncoders.of(resourceType.toCode());
    final Dataset<Row> updates = spark.createDataset(resources, encoder).toDF();
    merge(resourceType, updates);
  }

  /**
   * Creates or updates resources of the specified type by matching on ID.
   *
   * @param resourceType the type of resource to write
   * @param updates a {@link Dataset} containing the new or updated resource data
   */
  @ResourceAccess(AccessType.WRITE)
  public void merge(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> updates) {
    final DeltaTable original = readDelta(resourceType);

    log.debug("Writing updates to dataset: {}", resourceType.toCode());
    original
        .as("original")
        .merge(updates.as("updates"), "original.id = updates.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute();
  }

  /**
   * Checks that the resource has an ID that matches the supplied ID.
   *
   * @param resource the resource to be checked
   * @param id the ID supplied by the client
   * @return the resource
   */
  @Nonnull
  public static IBaseResource prepareResourceForUpdate(@Nonnull final IBaseResource resource,
      @Nonnull final String id) {
    checkUserInput(resource.getIdElement().getIdPart().equals(id),
        "Resource ID missing or does not match supplied ID");
    return resource;
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
      final String tableUrl = writeEmpty(resourceType);
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

  void write(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> resources, @Nonnull final SaveMode saveMode) {
    final String tableUrl = getTableUrl(warehouseUrl, databaseName, resourceType);

    // If the save mode is ErrorIfExists, we create the table in such a way that it will throw an
    // error if there is already a table at the specified path.
    // If the save mode is Overwrite, the table will be removed and replaced.
    final DeltaTableBuilder tableBuilder = saveMode.equals(SaveMode.ErrorIfExists)
                                           ? DeltaTable.create()
                                           : DeltaTable.createOrReplace(spark);

    // This step seems to be required to ensure that we don't get duplicated table contents when the 
    // server has been restarted between imports, or when used through the test data importer.
    // TODO: Work out why overwrite doesn't work as expected.
    if (saveMode.equals(SaveMode.Overwrite)) {
      log.debug("Deleting: {}", tableUrl);
      FileSystemUtils.deleteRecursively(new File(tableUrl.replaceFirst("file://", "")));
    }

    // We use the DeltaTableBuilder API to create the table with the required schema. This ensures 
    // that the table is created with an initial snapshot.
    log.debug("Creating table: {}", tableUrl);
    tableBuilder
        .addColumns(fhirEncoders.of(resourceType.toCode()).schema())
        .location(tableUrl)
        .execute();

    // We order the resources here to reduce the amount of sorting necessary at query time.
    log.debug("Writing resources to: {}", tableUrl);
    resources.orderBy(asc("id"))
        .write()
        .format("delta")
        .mode(SaveMode.Overwrite)
        // By default, Delta throws an error if the incoming schema is different to the existing 
        // one. For the purposes of this method, we want to be able to rewrite the schema in cases 
        // where it has changed, e.g. a version upgrade or a configuration change.
        // See: https://docs.delta.io/latest/delta-batch.html#replace-table-schema
        .option("overwriteSchema", "true")
        .save(tableUrl);
  }

  @Nonnull
  String writeEmpty(@Nonnull final ResourceType resourceType) {
    final Dataset<Row> dataset = createEmptyDataset(spark, fhirEncoders, resourceType);
    log.debug("Writing empty dataset: {}", resourceType.toCode());
    // We need to throw an error if the table already exists, otherwise we could get contention 
    // issues on requests that call this method.
    write(resourceType, dataset, SaveMode.ErrorIfExists);
    return getTableUrl(warehouseUrl, databaseName, resourceType);
  }

}