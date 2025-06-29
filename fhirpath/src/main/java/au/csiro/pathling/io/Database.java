/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.io;

import static au.csiro.pathling.QueryHelpers.createEmptyDataset;
import static au.csiro.pathling.fhir.FhirUtils.getResourceType;
import static au.csiro.pathling.io.FileSystemPersistence.safelyJoinPaths;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.asc;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.security.ResourceAccess;
import io.delta.tables.DeltaMergeBuilder;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Used for reading and writing resource data in persistent storage.
 *
 * @author John Grimes
 */
@Slf4j
public class Database implements DataSource {

  @Nonnull
  protected final SparkSession spark;

  @Nonnull
  protected final FhirEncoders fhirEncoders;

  @Nonnull
  protected final PersistenceScheme persistence;

  protected final boolean cacheDatasets;

  /**
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param persistence a {@link PersistenceScheme} object for reading and writing data
   * @param cacheDatasets whether to cache datasets in memory
   */
  public Database(@Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final PersistenceScheme persistence, final boolean cacheDatasets) {
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
    this.persistence = persistence;
    this.cacheDatasets = cacheDatasets;
  }

  /**
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param configuration a {@link StorageConfiguration} object which controls the behaviour of the
   * database
   * @return a new {@link Database} object
   */
  @Nonnull
  public static Database forConfiguration(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders, @Nonnull final StorageConfiguration configuration) {
    return new Database(spark, fhirEncoders, new FileSystemPersistence(
        spark, safelyJoinPaths(configuration.getWarehouseUrl(), configuration.getDatabaseName())),
        configuration.getCacheDatasets());
  }

  /**
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param path the path to the storage location, overriding the values of warehouse URL and
   * database name in the configuration
   * @param cacheDatasets whether to cache datasets in memory
   * @return a new {@link Database} object
   */
  public static Database forFileSystem(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders, @Nonnull final String path,
      final boolean cacheDatasets) {
    return new Database(spark, fhirEncoders, new FileSystemPersistence(spark, path),
        cacheDatasets);
  }

  /**
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param schema the name of the schema to use when qualifying table names
   * @param cacheDatasets whether to cache datasets in memory
   * @return a new {@link Database} object
   */
  public static Database forCatalog(@Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders, @Nonnull final Optional<String> schema,
      final boolean cacheDatasets) {
    return new Database(spark, fhirEncoders, new CatalogPersistence(spark, schema),
        cacheDatasets);
  }

  /**
   * Reads a data for the given resource type.
   *
   * @param resourceType the desired {@link ResourceType}
   * @return a {@link Dataset} containing the raw resource, i.e. NOT wrapped in a value column
   */
  @ResourceAccess(ResourceAccess.AccessType.READ)
  @Override
  @Nonnull
  public Dataset<Row> read(@Nullable final ResourceType resourceType) {
    return getMaybeNonExistentDeltaTable(requireNonNull(resourceType))
        .map(DeltaTable::toDF)
        // If there is no existing table, we return an empty table with the right shape.
        .orElseGet(() -> createEmptyDataset(spark, fhirEncoders, resourceType));
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    return read(getResourceType(resourceCode));
  }

  @Nonnull
  @Override
  public Set<ResourceType> getResourceTypes() {
    return persistence.list();
  }

  /**
   * Overwrites the resources for a particular type with the contents of the supplied
   * {@link Dataset}.
   *
   * @param resourceType the type of the resource to write
   * @param resources the {@link Dataset} containing the resource data
   */
  @ResourceAccess(ResourceAccess.AccessType.WRITE)
  public void overwrite(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> resources) {
    write(resourceType, resources);
    persistence.invalidate(resourceType);
  }

  /**
   * Creates or updates a single resource of the specified type by matching on ID.
   *
   * @param resourceType the type of resource to write
   * @param resource the new or updated resource
   */
  @ResourceAccess(ResourceAccess.AccessType.WRITE)
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
  @ResourceAccess(ResourceAccess.AccessType.WRITE)
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
  @ResourceAccess(ResourceAccess.AccessType.WRITE)
  public void merge(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> updates) {
    final DeltaTable original = readDelta(resourceType);

    log.debug("Writing updates: {}", resourceType.toCode());
    final DeltaMergeBuilder merge = original
        .as("original")
        .merge(updates.as("updates"), "original.id = updates.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll();
    persistence.merge(resourceType, merge);
    persistence.invalidate(resourceType);
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
    // When a `fullUrl` with a UUID is provided within a batch, the ID gets set to a URN. We need to 
    // convert this back to a naked ID before we use it in the update.
    final String originalId = resource.getIdElement().getIdPart();
    final String uuidPrefix = "urn:uuid:";
    if (originalId.startsWith(uuidPrefix)) {
      resource.setId(originalId.replaceFirst(uuidPrefix, ""));
    }
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
    return getMaybeNonExistentDeltaTable(resourceType).orElseGet(() -> {
      // If a Delta access is attempted upon a resource which does not yet exist, we create an empty 
      // table. This is because Delta operations cannot be done in absence of a persisted table.
      writeEmpty(resourceType);
      return getDeltaTable(resourceType);
    });
  }

  /**
   * Attempts to load a Delta table for the given resource type. Tables may not exist if they have
   * not previously been the subject of write operations.
   */
  private Optional<DeltaTable> getMaybeNonExistentDeltaTable(
      @Nonnull final ResourceType resourceType) {
    return persistence.exists(resourceType)
           ? Optional.of(getDeltaTable(resourceType))
           : Optional.empty();
  }

  /**
   * Loads a Delta table that we already know exists.
   */
  @Nonnull
  DeltaTable getDeltaTable(final @Nonnull ResourceType resourceType) {
    final DeltaTable resources = persistence.read(resourceType);

    if (cacheDatasets) {
      // Cache the raw resource data.
      log.debug("Caching resource dataset: {}", resourceType.toCode());
      resources.toDF().cache();
    }

    return resources;
  }

  void write(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> resources) {
    log.debug("Overwriting: {}", resourceType.toCode());

    // Check if table exists and drop it if schema needs to be overwritten.
    if (persistence.exists(resourceType)) {
      // Delete the existing table to avoid truncate issues.
      persistence.delete(resourceType);
    }

    final DataFrameWriter<Row> writer = resources
        .orderBy(asc("id"))
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists) // or SaveMode.Append for new table
        .option("overwriteSchema", "false"); // Remove this since we're creating fresh
    persistence.write(resourceType, writer);
  }

  void writeEmpty(@Nonnull final ResourceType resourceType) {
    final Dataset<Row> dataset = createEmptyDataset(spark, fhirEncoders, resourceType);
    log.debug("Writing empty dataset: {}", resourceType.toCode());
    // We need to throw an error if the table already exists, otherwise we could get contention 
    // issues on requests that call this method.
    write(resourceType, dataset);
  }

}
