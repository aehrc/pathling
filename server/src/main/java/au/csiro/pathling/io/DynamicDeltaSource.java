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

package au.csiro.pathling.io;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.library.query.FhirViewQuery;
import au.csiro.pathling.views.FhirView;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A QueryableDataSource wrapper that dynamically discovers new resource types created after
 * startup. Delegates to the underlying data source for known types, and attempts on-demand
 * discovery for unknown types by checking if a Delta table exists at the expected path.
 *
 * @author John Grimes
 */
@Slf4j
public class DynamicDeltaSource implements QueryableDataSource {

  @Nonnull private final QueryableDataSource delegate;

  @Nonnull private final SparkSession spark;

  @Nonnull private final String databasePath;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final Set<String> dynamicallyDiscoveredTypes = ConcurrentHashMap.newKeySet();

  /**
   * Constructs a new DynamicDeltaSource.
   *
   * @param delegate the underlying QueryableDataSource to delegate to
   * @param spark the Spark session for Delta table operations
   * @param databasePath the path to the Delta database
   * @param fhirEncoders the FHIR encoders for creating empty datasets
   */
  public DynamicDeltaSource(
      @Nonnull final QueryableDataSource delegate,
      @Nonnull final SparkSession spark,
      @Nonnull final String databasePath,
      @Nonnull final FhirEncoders fhirEncoders) {
    this.delegate = delegate;
    this.spark = spark;
    this.databasePath = databasePath;
    this.fhirEncoders = fhirEncoders;
  }

  @Override
  @Nonnull
  public Dataset<Row> read(@Nonnull final String resourceCode) {
    // If delegate knows about this type, use it.
    if (delegate.getResourceTypes().contains(resourceCode)) {
      return delegate.read(resourceCode);
    }

    // If we've already discovered this type dynamically, read from Delta.
    if (dynamicallyDiscoveredTypes.contains(resourceCode)) {
      return readFromDelta(resourceCode);
    }

    // Try to discover the Delta table.
    final String tablePath = getTablePath(resourceCode);
    if (DeltaTable.isDeltaTable(spark, tablePath)) {
      log.debug("Dynamically discovered Delta table for resource type: {}", resourceCode);
      dynamicallyDiscoveredTypes.add(resourceCode);
      return readFromDelta(resourceCode);
    }

    // No data found - return an empty dataset with the correct schema.
    log.debug("No data found for resource type: {}, returning empty dataset", resourceCode);
    return QueryHelpers.createEmptyDataset(
        spark, fhirEncoders, ResourceType.fromCode(resourceCode));
  }

  @Override
  @Nonnull
  public Set<String> getResourceTypes() {
    final Set<String> types = new HashSet<>(delegate.getResourceTypes());
    types.addAll(dynamicallyDiscoveredTypes);
    return types;
  }

  @Override
  @Nonnull
  public DataSinkBuilder write() {
    return delegate.write();
  }

  @Override
  @Nonnull
  public FhirViewQuery view(@Nullable final String subjectResource) {
    return delegate.view(subjectResource);
  }

  @Override
  @Nonnull
  public FhirViewQuery view(@Nullable final FhirView view) {
    return delegate.view(view);
  }

  @Override
  @Nonnull
  public QueryableDataSource map(
      @Nonnull final BiFunction<String, Dataset<Row>, Dataset<Row>> operator) {
    return delegate.map(operator);
  }

  @Override
  @Nonnull
  public QueryableDataSource filterByResourceType(
      @Nonnull final Predicate<String> resourceTypePredicate) {
    return delegate.filterByResourceType(resourceTypePredicate);
  }

  @Override
  @Nonnull
  public DataSource cache() {
    return delegate.cache();
  }

  @Nonnull
  private Dataset<Row> readFromDelta(@Nonnull final String resourceCode) {
    final String tablePath = getTablePath(resourceCode);
    return spark.read().format("delta").load(tablePath);
  }

  @Nonnull
  private String getTablePath(@Nonnull final String resourceCode) {
    return FileSystemPersistence.safelyJoinPaths(databasePath, resourceCode + ".parquet");
  }
}
