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

package au.csiro.pathling.library.data;

import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.query.ImmutableDataSource;
import au.csiro.pathling.query.ImmutableDataSource.Builder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Table;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import scala.collection.JavaConverters;

/**
 * A builder for creating a data source from the Spark table catalog.
 *
 * @author John Grimes
 */
public class CatalogSourceBuilder extends AbstractSourceBuilder<CatalogSourceBuilder> {

  private final Map<String, ResourceType> LOWER_CASE_RESOURCE_NAMES;

  {
    final Set<String> unsupported = JavaConverters.setAsJavaSet(
        EncoderBuilder.UNSUPPORTED_RESOURCES());
    LOWER_CASE_RESOURCE_NAMES = Arrays.stream(ResourceType.values())
        .filter(resourceType -> !unsupported.contains(resourceType.name()))
        .filter(resourceType -> !resourceType.equals(ResourceType.NULL))
        .collect(Collectors.toMap(
            resourceType -> resourceType.name().toLowerCase(),
            resourceType -> resourceType
        ));
  }

  @Nullable
  private String schema;

  protected CatalogSourceBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
  }

  /**
   * Sets the schema from which the tables will be loaded.
   *
   * @param schema the schema name
   * @return this builder
   */
  @Nonnull
  public CatalogSourceBuilder withSchema(@Nullable final String schema) {
    this.schema = schema;
    return this;
  }

  /**
   * @return a new data source, built using the Spark table catalog and the supplied options
   */
  @Nonnull
  @Override
  protected DataSource buildDataSource() {
    final Builder inMemoryDataSourceBuilder = ImmutableDataSource.builder();

    for (final Table table : getTables()) {
      // Check if the name of the table is a valid resource type, and is not one of the unsupported 
      // resources.
      final String tableName = table.name();

      // The comparison of the table name to the resource type is case-insensitive.
      @Nullable final ResourceType resourceType = LOWER_CASE_RESOURCE_NAMES.get(
          tableName.toLowerCase());
      if (resourceType == null) {
        // If the table name is not a valid resource type, then skip it.
        continue;
      }

      // If a schema is specified, then qualify the table name with it.
      final String qualifiedTableName;
      if (schema == null) {
        qualifiedTableName = tableName;
      } else {
        qualifiedTableName = String.join(".", schema, tableName);
      }

      // Read the table from the catalog.
      final Dataset<Row> tableDataset = pathlingContext.getSpark().read()
          .table(qualifiedTableName);

      // Use the InMemoryDataSourceBuilder to build the data source using the dataset read from
      // the catalog.
      inMemoryDataSourceBuilder.withResource(resourceType, tableDataset);
    }

    return inMemoryDataSourceBuilder.build();
  }

  /**
   * @return the list of tables from the catalog, using the specified schema if one is provided
   */
  private List<Table> getTables() {
    final Catalog catalog = pathlingContext.getSpark().catalog();
    final Dataset<Table> tablesDataset;
    try {
      tablesDataset = schema == null
                      ? catalog.listTables()
                      : catalog.listTables(schema);
    } catch (final AnalysisException e) {
      throw new RuntimeException("Specified schema was not found", e);
    }
    return tablesDataset.collectAsList();
  }

}
