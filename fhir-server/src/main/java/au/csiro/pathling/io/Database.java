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
import static au.csiro.pathling.io.PersistenceScheme.convertS3ToS3aUrl;
import static au.csiro.pathling.io.PersistenceScheme.getTableUrl;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.desc;

import au.csiro.pathling.caching.Cacheable;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.security.PathlingAuthority.AccessType;
import au.csiro.pathling.security.ResourceAccess;
import io.delta.tables.DeltaTable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Used for reading and writing resource data in persistent storage.
 *
 * @author John Grimes
 */
@Component
@Profile("(core | import) & !ga4gh")
@Slf4j
public class Database implements DataSource, Cacheable {

  @Nonnull
  @Getter
  private Optional<String> cacheKey;

  @Nonnull
  private final String warehouseUrl;

  @Nonnull
  private final String databaseName;

  @Nonnull
  private final StorageConfiguration configuration;

  @Nonnull
  protected final SparkSession spark;

  @Nonnull
  protected final FhirEncoders fhirEncoders;

  @Nonnull
  protected final ThreadPoolTaskExecutor executor;

  /**
   * @param configuration a {@link StorageConfiguration} object which controls the behaviour of the
   * database
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param executor a {@link ThreadPoolTaskExecutor} for executing asynchronous tasks
   */
  public Database(@Nonnull final StorageConfiguration configuration,
      @Nonnull final SparkSession spark, @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ThreadPoolTaskExecutor executor) {
    this.configuration = configuration;
    this.spark = spark;
    this.warehouseUrl = convertS3ToS3aUrl(configuration.getWarehouseUrl());
    this.databaseName = configuration.getDatabaseName();
    this.fhirEncoders = fhirEncoders;
    this.executor = executor;
    cacheKey = buildCacheKeyFromDatabase();
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
    write(resourceType, resources);
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

    log.debug("Writing updates: {}", resourceType.toCode());
    original
        .as("original")
        .merge(updates.as("updates"), "original.id = updates.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute();

    final String tableUrl = getTableUrl(warehouseUrl, databaseName, resourceType);
    invalidateCache(tableUrl);
    compact(resourceType, original);
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

  @Override
  public boolean cacheKeyMatches(@Nonnull final String otherKey) {
    return cacheKey.map(key -> key.equals(otherKey)).orElse(false);
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
    requireNonNull(resources);

    if (configuration.getCacheDatasets()) {
      // Cache the raw resource data.
      log.debug("Caching resource dataset: {}", resourceType.toCode());
      resources.toDF().cache();
    }

    return resources;
  }

  void write(@Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> resources) {
    final String tableUrl = getTableUrl(warehouseUrl, databaseName, resourceType);

    log.debug("Overwriting: {}", tableUrl);
    resources
        // We order the resources here to reduce the amount of sorting necessary at query time.
        .orderBy(asc("id"))
        .write()
        .format("delta")
        .mode(SaveMode.Overwrite)
        // By default, Delta throws an error if the incoming schema is different to the existing 
        // one. For the purposes of this method, we want to be able to rewrite the schema in cases 
        // where it has changed, e.g. a version upgrade or a configuration change.
        // See: https://docs.delta.io/latest/delta-batch.html#replace-table-schema
        .option("overwriteSchema", "true")
        .save(tableUrl);

    invalidateCache(tableUrl);
  }

  @Nonnull
  String writeEmpty(@Nonnull final ResourceType resourceType) {
    final Dataset<Row> dataset = createEmptyDataset(spark, fhirEncoders, resourceType);
    log.debug("Writing empty dataset: {}", resourceType.toCode());
    // We need to throw an error if the table already exists, otherwise we could get contention 
    // issues on requests that call this method.
    write(resourceType, dataset);
    return getTableUrl(warehouseUrl, databaseName, resourceType);
  }

  /**
   * Compacts the table if it has a number of partitions that exceed the configured threshold.
   *
   * @param resourceType the resource type of the table to compact
   * @param table the Delta table for which to check the number of partitions
   * @see <a href="https://docs.delta.io/latest/best-practices.html#compact-files">Delta Lake
   * Documentation - Compact files</a>
   */
  private void compact(final @Nonnull ResourceType resourceType, final DeltaTable table) {
    final int threshold = configuration.getCompactionThreshold();
    final int numPartitions = table.toDF().rdd().getNumPartitions();
    if (numPartitions > threshold) {
      final String tableUrl = getTableUrl(warehouseUrl, databaseName, resourceType);
      log.debug("Scheduling table compaction (number of partitions: {}, threshold: {}): {}",
          numPartitions,
          threshold, tableUrl);
      executor.submit(() -> {
        log.debug("Commencing compaction: {}", tableUrl);
        read(resourceType)
            .repartition()
            .write()
            .option("dataChange", "false")
            .format("delta")
            .mode(SaveMode.Overwrite)
            .save(tableUrl);
        log.debug("Compaction complete: {}", tableUrl);
      });
    } else {
      log.debug("Compaction not needed (number of partitions: {}, threshold: {})", numPartitions,
          threshold);
    }
  }

  private void invalidateCache(final String tableUrl) {
    executor.execute(() -> {
      cacheKey = buildCacheKeyFromTable(tableUrl);
      spark.sqlContext().clearCache();
    });
  }

  private Optional<String> buildCacheKeyFromDatabase() {
    return latestUpdateToDatabase().map(this::cacheKeyFromTimestamp);
  }

  /**
   * Checks the warehouse location and gets the latest snapshot timestamp found within all the
   * tables.
   */
  private Optional<Long> latestUpdateToDatabase() {
    final String databasePath = warehouseUrl + "/" + databaseName;
    log.info("Querying latest snapshot from database: {}", databasePath);

    @Nullable final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark.sparkContext()
        .hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouse;
    try {
      warehouse = FileSystem.get(new URI(warehouseUrl), hadoopConfiguration);
    } catch (final IOException | URISyntaxException e) {
      log.debug("Unable to access warehouse location, returning empty snapshot time: {}",
          warehouseUrl);
      return Optional.empty();
    }
    requireNonNull(warehouse);

    // Check that the database path exists.
    try {
      warehouse.exists(new Path(databasePath));
    } catch (final IOException e) {
      log.debug("Unable to access database location, returning empty snapshot time: {}",
          databasePath);
      return Optional.empty();
    }

    // Find all the Parquet files within the warehouse and use them to create a set of resource
    // types.
    @Nullable final FileStatus[] fileStatuses;
    try {
      fileStatuses = warehouse.listStatus(new Path(databasePath));
    } catch (final IOException e) {
      log.debug("Unable to access database location, returning empty snapshot time: {}",
          databasePath);
      return Optional.empty();
    }
    requireNonNull(fileStatuses);

    final List<Long> timestamps = Arrays.stream(fileStatuses)
        // Get the filename of each item in the directory listing.
        .map(fileStatus -> {
          @Nullable final Path path = fileStatus.getPath();
          requireNonNull(path);
          return path.toString();
        })
        // Filter out any file names that don't match the pattern.
        .filter(path -> path.matches("^[^.]+\\.parquet$"))
        // Filter out anything that is not a Delta table.
        .filter(path -> DeltaTable.isDeltaTable(spark, path))
        // Get the latest history entry for each Delta table.
        .map(this::latestUpdateToTable)
        // Filter out any tables which don't have history rows.
        .filter(Optional::isPresent)
        // Get the timestamp from the history row.
        .map(Optional::get)
        .collect(Collectors.toList());

    return timestamps.isEmpty()
           ? Optional.empty()
           : Optional.ofNullable(Collections.max(timestamps));
  }

  @Nonnull
  private Optional<String> buildCacheKeyFromTable(@Nonnull final String path) {
    return latestUpdateToTable(path).map(this::cacheKeyFromTimestamp);
  }

  @Nonnull
  private Optional<Long> latestUpdateToTable(@Nonnull final String path) {
    log.debug("Querying latest snapshot for table: {}", path);
    final DeltaTable deltaTable = DeltaTable.forPath(spark, path);
    @SuppressWarnings("RedundantCast") final Row[] head = (Row[]) deltaTable.history()
        .orderBy(desc("version"))
        .select("timestamp")
        .head(1);
    if (head.length != 1) {
      return Optional.empty();
    } else {
      return Optional.of(head[0].getTimestamp(0).getTime());
    }
  }

  @Nonnull
  private String cacheKeyFromTimestamp(@Nonnull final Long timestamp) {
    return Long.toString(timestamp, Character.MAX_RADIX);
  }

}
