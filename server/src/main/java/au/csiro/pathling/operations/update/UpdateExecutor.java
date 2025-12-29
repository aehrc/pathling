/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final String databasePath;

  @Nonnull private final CacheableDatabase cacheableDatabase;

  /**
   * Constructs a new UpdateExecutor.
   *
   * @param pathlingContext the Pathling context for Spark operations
   * @param fhirEncoders encoders for converting FHIR resources to Spark Datasets
   * @param databasePath the path to the Delta database
   * @param cacheableDatabase the cacheable database for cache invalidation
   */
  public UpdateExecutor(
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final FhirEncoders fhirEncoders,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}") @Nonnull
          final String databasePath,
      @Nonnull final CacheableDatabase cacheableDatabase) {
    this.pathlingContext = pathlingContext;
    this.fhirEncoders = fhirEncoders;
    this.databasePath = databasePath;
    this.cacheableDatabase = cacheableDatabase;
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
   * @param resourceCode the type code of the resources (e.g., "Patient", "ViewDefinition")
   * @param resources the resources to merge
   */
  public void merge(
      @Nonnull final String resourceCode, @Nonnull final List<IBaseResource> resources) {
    if (resources.isEmpty()) {
      return;
    }

    final SparkSession spark = pathlingContext.getSpark();
    final Dataset<Row> updates =
        spark.createDataset(resources, fhirEncoders.of(resourceCode)).toDF();

    log.debug("Merging {} resource(s) of type {}", resources.size(), resourceCode);
    final String tablePath = getTablePath(resourceCode);

    if (deltaTableExists(spark, tablePath)) {
      // Perform a merge operation on the existing table.
      final DeltaTable table = DeltaTable.forPath(spark, tablePath);
      table
          .as("original")
          .merge(updates.as("updates"), "original.id = updates.id")
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute();
    } else {
      // Create a new table with the resources.
      log.debug("Creating new Delta table for resource type: {}", resourceCode);
      updates.write().format("delta").mode(SaveMode.ErrorIfExists).save(tablePath);
    }

    // Invalidate the cache to ensure subsequent requests see the updated data. Use the optimised
    // single-table invalidation since we know exactly which table was modified.
    cacheableDatabase.invalidate(tablePath);
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
   * Checks if a Delta table exists at the specified path.
   *
   * @param spark the Spark session
   * @param tablePath the path to check
   * @return true if a Delta table exists at the path
   */
  private static boolean deltaTableExists(
      @Nonnull final SparkSession spark, @Nonnull final String tablePath) {
    return DeltaTable.isDeltaTable(spark, tablePath);
  }
}
