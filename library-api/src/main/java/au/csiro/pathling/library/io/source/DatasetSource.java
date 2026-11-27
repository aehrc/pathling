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

package au.csiro.pathling.library.io.source;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.io.SchemaSet;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A class for making FHIR data with Spark datasets available for query.
 *
 * <p>The datasets are expected in the Parquet on FHIR layout. Their layout is not classified. A
 * dataset in the layout written by earlier releases, such as one built with the encoder of those
 * releases, is not supported, and the results of a query over it are undefined.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class DatasetSource extends AbstractSource {

  /**
   * A map of resource codes to their corresponding datasets. The key is the resource code, and the
   * value is the dataset containing the resource data.
   */
  @Nonnull protected Map<String, Dataset<Row>> resourceMap = new HashMap<>();

  /** Projects each dataset added of a type the schema set names onto the set's schema. */
  @Nonnull private final DefinedSchemaReads reads;

  /**
   * Constructs a DatasetSource with the specified PathlingContext.
   *
   * @param context the PathlingContext to use
   */
  public DatasetSource(@Nonnull final PathlingContext context) {
    this(context, DefinedSchemaReads.noSchemas());
  }

  /**
   * Constructs a DatasetSource with the specified PathlingContext, projecting each dataset added of
   * a resource type a schema set names onto its schema.
   *
   * @param context the PathlingContext to use
   * @param schemaSet the schema set, which may be empty
   */
  DatasetSource(@Nonnull final PathlingContext context, @Nonnull final SchemaSet schemaSet) {
    super(context);
    this.reads = DefinedSchemaReads.of(context, schemaSet);
  }

  /**
   * Constructs a DatasetSource holding datasets already added, which projects each dataset added to
   * it later as the source it was derived from does.
   */
  private DatasetSource(
      @Nonnull final PathlingContext context,
      @Nonnull final Map<String, Dataset<Row>> resourceMap,
      @Nonnull final DefinedSchemaReads reads) {
    super(context);
    this.reads = reads;
    this.resourceMap = resourceMap;
  }

  /**
   * Add a dataset to this source. A dataset of a resource type the source's schema set names is
   * projected onto the set's schema as it is added.
   *
   * @param resourceType the resource code
   * @param dataset the dataset
   * @return this data source, for chaining
   * @throws IllegalArgumentException if a field of the dataset differs from a field of the set's
   *     schema only in case, or a struct of it holds two fields differing only in case
   */
  public DatasetSource dataset(
      @Nullable final String resourceType, @Nullable final Dataset<Row> dataset) {
    requireNonNull(resourceType);
    requireNonNull(dataset);
    resourceMap.put(resourceType, reads.project(dataset, resourceType));
    return this;
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    return Optional.ofNullable(resourceCode)
        .map(resourceMap::get)
        .orElseThrow(
            () -> new IllegalArgumentException("No data found for resource type: " + resourceCode));
  }

  @Override
  public @Nonnull Set<String> getResourceTypes() {
    return resourceMap.keySet();
  }

  @Nonnull
  @Override
  public QueryableDataSource map(
      @Nonnull final BiFunction<String, Dataset<Row>, Dataset<Row>> operator) {
    final Map<String, Dataset<Row>> transformedMap = new HashMap<>();
    for (final Map.Entry<String, Dataset<Row>> entry : resourceMap.entrySet()) {
      final String resourceType = entry.getKey();
      final Dataset<Row> dataset = entry.getValue();
      transformedMap.put(resourceType, operator.apply(entry.getKey(), dataset));
    }
    return new DatasetSource(context, transformedMap, reads);
  }

  @Override
  public @Nonnull QueryableDataSource filterByResourceType(
      @Nonnull final Predicate<String> resourceTypePredicate) {
    final Map<String, Dataset<Row>> filteredMap =
        resourceMap.entrySet().stream()
            .filter(entry -> resourceTypePredicate.test(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return new DatasetSource(context, filteredMap, reads);
  }

  @Override
  public QueryableDataSource cache() {
    return map(Dataset::cache);
  }
}
