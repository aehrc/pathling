/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import static java.util.stream.Collectors.toSet;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.PersistenceError;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Table;

/**
 * A class for making FHIR data in the Spark catalog available for query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class CatalogSource extends AbstractSource {

  @Nonnull private final Optional<String> schema;

  @Nonnull private final Optional<UnaryOperator<Dataset<Row>>> transformation;

  /**
   * Constructs a CatalogSource with the specified PathlingContext and an empty schema.
   *
   * @param context the PathlingContext to use
   */
  CatalogSource(@Nonnull final PathlingContext context) {
    super(context);
    this.schema = Optional.empty(); // Default schema is empty
    this.transformation = Optional.empty(); // No transformation by default
  }

  /**
   * Constructs a CatalogSource with the specified PathlingContext and schema.
   *
   * @param context the PathlingContext to use
   * @param schema the schema to use for the catalog tables
   */
  CatalogSource(@Nonnull final PathlingContext context, @Nonnull final String schema) {
    super(context);
    this.schema = Optional.of(schema);
    this.transformation = Optional.empty(); // No transformation by default
  }

  private CatalogSource(
      @Nonnull final PathlingContext context,
      @Nonnull final Optional<String> schema,
      @Nonnull final Optional<UnaryOperator<Dataset<Row>>> transformation) {
    super(context);
    this.schema = schema;
    this.transformation = transformation;
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    requireNonNull(resourceCode);
    final Dataset<Row> table = context.getSpark().table(getTableName(resourceCode));
    // If a transformation is provided, apply it to the table.
    // Otherwise, return the table as is.
    return transformation.map(t -> t.apply(table)).orElse(table);
  }

  @Nonnull
  @Override
  public Set<String> getResourceTypes() {
    return getTables().stream()
        // Get all the names of the tables in the target schema.
        .map(Table::name)
        .filter(Objects::nonNull)
        // Match the name of each table to a supported resource type.
        .map(context::matchSupportedResourceType)
        // Filter out any tables that don't match a supported resource type.
        .filter(Optional::isPresent)
        // Swap the table names for the canonical resource code (upper camel case).
        .map(Optional::get)
        .collect(toSet());
  }

  /**
   * @param resourceType the resource type to get the table name for
   * @return the name of the table for the given resource type, qualified by the specified schema if
   *     one is provided
   */
  @Nonnull
  private String getTableName(@Nonnull final String resourceType) {
    return schema.map(s -> String.join(".", s, resourceType)).orElse(resourceType);
  }

  /**
   * @return the list of tables from the catalog, using the specified schema if one is provided
   * @throws PersistenceError if the specified schema does not exist
   */
  private List<Table> getTables() {
    final Catalog catalog = context.getSpark().catalog();
    final Dataset<Table> tablesDataset;
    // If a schema is provided, use it to list the tables. Otherwise, list tables from the currently
    // selected schema.
    tablesDataset =
        schema
            .map(
                dbName -> {
                  try {
                    return catalog.listTables(dbName);
                  } catch (final AnalysisException e) {
                    throw new PersistenceError("Specified schema was not found", e);
                  }
                })
            .orElseGet(catalog::listTables);
    return tablesDataset.collectAsList();
  }

  @Nonnull
  @Override
  public CatalogSource map(@Nonnull final UnaryOperator<Dataset<Row>> operator) {
    return new CatalogSource(context, schema, Optional.of(operator));
  }

  @Override
  public CatalogSource cache() {
    return map(Dataset::cache);
  }
}
