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

package au.csiro.pathling.operations.delete;

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.library.PathlingContext;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Executes delete operations by removing resources from Delta Lake tables.
 *
 * @author John Grimes
 */
@Component
@Slf4j
public class DeleteExecutor {

  @Nonnull private final PathlingContext pathlingContext;

  @Nonnull private final String databasePath;

  @Nonnull private final CacheableDatabase cacheableDatabase;

  /**
   * Constructs a new DeleteExecutor.
   *
   * @param pathlingContext the Pathling context for Spark operations
   * @param databasePath the path to the Delta database
   * @param cacheableDatabase the cacheable database for cache invalidation
   */
  public DeleteExecutor(
      @Nonnull final PathlingContext pathlingContext,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}") @Nonnull
          final String databasePath,
      @Nonnull final CacheableDatabase cacheableDatabase) {
    this.pathlingContext = pathlingContext;
    this.databasePath = databasePath;
    this.cacheableDatabase = cacheableDatabase;
  }

  /**
   * Deletes a single resource from the Delta table for its type.
   *
   * @param resourceCode the type code of the resource (e.g., "Patient", "ViewDefinition")
   * @param resourceId the logical ID of the resource to delete
   * @throws ResourceNotFoundError if the resource does not exist
   */
  public void delete(@Nonnull final String resourceCode, @Nonnull final String resourceId) {
    final SparkSession spark = pathlingContext.getSpark();
    final String tablePath = getTablePath(resourceCode);

    // Check if Delta table exists.
    if (!DeltaTable.isDeltaTable(spark, tablePath)) {
      throw new ResourceNotFoundError("Resource not found: " + resourceCode + "/" + resourceId);
    }

    final DeltaTable table = DeltaTable.forPath(spark, tablePath);

    // Check if resource exists before deleting.
    final long count = table.toDF().filter(col("id").equalTo(resourceId)).count();
    if (count == 0) {
      throw new ResourceNotFoundError("Resource not found: " + resourceCode + "/" + resourceId);
    }

    // Perform deletion.
    log.debug("Deleting {} with ID: {}", resourceCode, resourceId);
    table.delete(col("id").equalTo(resourceId));

    // Invalidate the cache to ensure subsequent requests see the updated data. Use the optimised
    // single-table invalidation since we know exactly which table was modified.
    cacheableDatabase.invalidate(tablePath);
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
}
