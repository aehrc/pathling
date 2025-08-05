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

package au.csiro.pathling.library.io.source;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.PersistenceError;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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

  @Nonnull
  private final Optional<String> schema;

  /**
   * Constructs a CatalogSource with the specified PathlingContext and an empty schema.
   *
   * @param context the PathlingContext to use
   */
  public CatalogSource(@Nonnull final PathlingContext context) {
    super(context);
    this.schema = Optional.empty(); // Default schema is empty
  }

  /**
   * Constructs a CatalogSource with the specified PathlingContext and schema.
   *
   * @param context the PathlingContext to use
   * @param schema the schema to use for the catalog tables
   */
  public CatalogSource(@Nonnull final PathlingContext context, @Nonnull final String schema) {
    super(context);
    this.schema = Optional.of(schema);
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    requireNonNull(resourceCode);
    return context.getSpark().table(getTableName(resourceCode));
  }

  @Nonnull
  @Override
  public Set<String> getResourceTypes() {
    return getTables().stream()
        .map(Table::name)
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  /**
   * @param resourceType the resource type to get the table name for
   * @return the name of the table for the given resource type, qualified by the specified schema if
   * one is provided
   */
  @Nonnull
  private String getTableName(@Nonnull final String resourceType) {
    return schema.map(s -> String.join(".", s, resourceType))
        .orElse(resourceType);
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
    tablesDataset = schema.map(dbName -> {
          try {
            return catalog.listTables(dbName);
          } catch (final AnalysisException e) {
            throw new PersistenceError("Specified schema was not found", e);
          }
        })
        .orElseGet(catalog::listTables);
    return tablesDataset.collectAsList();
  }

}
