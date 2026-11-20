/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.update;

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.io.ResourceEncoding;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Executes update operations by merging resources into Delta Lake tables.
 *
 * @author John Grimes
 */
@Component
@Slf4j
public class UpdateExecutor {

  @Nonnull private final PathlingContext pathlingContext;

  @Nonnull private final String databasePath;

  @Nonnull private final CacheableDatabase cacheableDatabase;

  @Nonnull private final QueryableDataSource dataSource;

  /**
   * Per-resource-code locks serialising writes that create a new table in {@link #merge(String,
   * List)}. The library's merge creates a missing table with a write that fails if the table
   * already exists, so without this, concurrent first-creates of the same resource type could all
   * find the table missing and race the initial write, failing every loser with an error. Inside
   * the lock, the loser of a race finds the table already created and merges into it instead. This
   * executor is a singleton component, so an instance-scoped map covers the whole server process;
   * cross-process coordination is out of scope.
   */
  @Nonnull
  private final ConcurrentMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

  /**
   * Constructs a new UpdateExecutor.
   *
   * @param pathlingContext the Pathling context, used to encode and write the resources
   * @param databasePath the path to the Delta database
   * @param cacheableDatabase the cacheable database for cache invalidation
   * @param dataSource the data source serving queries, refreshed after a write that changes a
   *     table's schema
   */
  public UpdateExecutor(
      @Nonnull final PathlingContext pathlingContext,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}") @Nonnull
          final String databasePath,
      @Nonnull final CacheableDatabase cacheableDatabase,
      @Nonnull final QueryableDataSource dataSource) {
    this.pathlingContext = pathlingContext;
    this.databasePath = databasePath;
    this.cacheableDatabase = cacheableDatabase;
    this.dataSource = dataSource;
  }

  /**
   * Merges a single resource into the Delta table for its type.
   *
   * @param resourceType the type of resource being updated
   * @param resource the resource to merge
   */
  public void merge(
      @Nonnull final ResourceType resourceType, @Nonnull final IBaseResource resource) {
    merge(resourceType, List.of(resource));
  }

  /**
   * Merges multiple resources of the same type into the Delta table.
   *
   * @param resourceType the type of resources being updated
   * @param resources the resources to merge
   */
  public void merge(
      @Nonnull final ResourceType resourceType, @Nonnull final List<IBaseResource> resources) {
    merge(resourceType.toCode(), resources);
  }

  /**
   * Merges a single resource into the Delta table using a string resource code. This method
   * supports custom resource types like ViewDefinition that are not part of the standard FHIR
   * ResourceType enum.
   *
   * @param resourceCode the type code of the resource (e.g., "Patient", "ViewDefinition")
   * @param resource the resource to merge
   */
  public void merge(@Nonnull final String resourceCode, @Nonnull final IBaseResource resource) {
    merge(resourceCode, List.of(resource));
  }

  /**
   * Merges multiple resources of the same type into the Delta table using a string resource code.
   * This method supports custom resource types like ViewDefinition that are not part of the
   * standard FHIR ResourceType enum.
   *
   * <p>The resources are upserted on their id through the library's Delta sink, which creates the
   * table if it is missing and widens its schema to admit elements it has not stored before. The
   * sink refuses to write into a table in the legacy layout.
   *
   * @param resourceCode the type code of the resources (e.g., "Patient", "ViewDefinition")
   * @param resources the resources to merge
   */
  public void merge(
      @Nonnull final String resourceCode, @Nonnull final List<IBaseResource> resources) {
    if (resources.isEmpty()) {
      return;
    }

    final SparkSession spark = pathlingContext.getSpark();
    final Dataset<Row> updates = ResourceEncoding.encode(pathlingContext, resourceCode, resources);

    log.debug("Merging {} resource(s) of type {}", resources.size(), resourceCode);
    final String tablePath = getTablePath(resourceCode);
    final Optional<StructType> schemaBefore = tableSchema(spark, tablePath);

    if (schemaBefore.isPresent()) {
      write(resourceCode, updates);
    } else {
      // Serialise first-creates of this resource type so that concurrent requests cannot race the
      // initial write.
      final Object creationLock =
          tableCreationLocks.computeIfAbsent(resourceCode, key -> new Object());
      synchronized (creationLock) {
        write(resourceCode, updates);
      }
    }

    // Invalidate the cache to ensure subsequent requests see the updated data. Use the optimised
    // single-table invalidation since we know exactly which table was modified.
    cacheableDatabase.invalidate(tablePath);

    // A dataset loaded from a Delta table keeps the schema it was loaded with, so after a write
    // that changed the table's schema, refresh the dataset served for this resource type so that
    // in-process consumers can see the new elements. Comparing the schemas rather than tracking
    // whether the write widened the table also covers a table created or widened by a concurrent
    // writer.
    if (!tableSchema(spark, tablePath).equals(schemaBefore)
        && dataSource instanceof final DynamicDeltaSource dynamicDataSource) {
      dynamicDataSource.refresh(resourceCode);
    }
  }

  /**
   * Upserts resources into the Delta table for their type, matching on id.
   *
   * @param resourceCode the type code of the resources
   * @param updates the encoded resources
   */
  private void write(@Nonnull final String resourceCode, @Nonnull final Dataset<Row> updates) {
    final DatasetSource source = new DatasetSource(pathlingContext).dataset(resourceCode, updates);
    new DataSinkBuilder(pathlingContext, source).saveMode("merge").delta(databasePath);
  }

  /**
   * Prepares a resource for update by validating that its ID matches the supplied ID. Handles
   * conversion of UUID-prefixed IDs.
   *
   * @param resource the resource to prepare
   * @param id the expected ID
   * @return the prepared resource
   */
  @Nonnull
  public static IBaseResource prepareResourceForUpdate(
      @Nonnull final IBaseResource resource, @Nonnull final String id) {
    // When a fullUrl with a UUID is provided within a batch, the ID gets set to a URN. We need to
    // convert this back to a naked ID before we use it in the update.
    final String originalId = resource.getIdElement().getIdPart();
    final String uuidPrefix = "urn:uuid:";
    if (originalId != null && originalId.startsWith(uuidPrefix)) {
      resource.setId(originalId.replaceFirst(uuidPrefix, ""));
    }
    checkUserInput(
        resource.getIdElement().getIdPart() != null
            && resource.getIdElement().getIdPart().equals(id),
        "Resource ID missing or does not match supplied ID");
    return resource;
  }

  /**
   * Gets the path to the Delta table for a given resource type.
   *
   * @param resourceCode the resource type code
   * @return the table path
   */
  @Nonnull
  private String getTablePath(@Nonnull final String resourceCode) {
    return safelyJoinPaths(databasePath, resourceCode + ".parquet");
  }

  /**
   * Returns the schema of the Delta table at the specified path, if one exists there.
   *
   * @param spark the Spark session
   * @param tablePath the path to check
   * @return the schema of the table, or empty if there is no Delta table at the path
   */
  @Nonnull
  private static Optional<StructType> tableSchema(
      @Nonnull final SparkSession spark, @Nonnull final String tablePath) {
    return DeltaTable.isDeltaTable(spark, tablePath)
        ? Optional.of(DeltaTable.forPath(spark, tablePath).toDF().schema())
        : Optional.empty();
  }
}
