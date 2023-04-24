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

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.query.DataSource;
import au.csiro.pathling.query.ImmutableDataSource;
import au.csiro.pathling.query.ImmutableDataSource.Builder;
import java.util.Set;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A builder for creating a data source from the Spark table catalog.
 *
 * @author John Grimes
 */
public class CatalogSourceBuilder extends AbstractSourceBuilder<CatalogSourceBuilder> {

  @Nonnull
  private UnaryOperator<String> tableNameMapper;

  @Nonnull
  private Set<ResourceType> resourceTypes;

  @Nullable
  private String schema;

  protected CatalogSourceBuilder(@Nonnull final PathlingContext pathlingContext) {
    super(pathlingContext);
    tableNameMapper = UnaryOperator.identity();
    resourceTypes = Set.of();
  }

  /**
   * Sets a function that will be used to map the resource type to the name of the table in the
   * catalog.
   *
   * @param tableNameMapper the function to use
   * @return this builder
   */
  @Nonnull
  public CatalogSourceBuilder withTableNameMapper(
      @Nonnull final UnaryOperator<String> tableNameMapper) {
    this.tableNameMapper = tableNameMapper;
    return this;
  }

  /**
   * Sets the resource types that will be loaded from the catalog.
   *
   * @param resourceTypes a set of resource types
   * @return this builder
   */
  @Nonnull
  public CatalogSourceBuilder withResourceTypes(@Nonnull final Set<ResourceType> resourceTypes) {
    this.resourceTypes = resourceTypes;
    return this;
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

  @Nonnull
  @Override
  protected DataSource buildDataSource() {
    final Builder inMemoryDataSourceBuilder = ImmutableDataSource.builder();

    for (final ResourceType resourceType : resourceTypes) {
      // If a schema is specified, then qualify the table name with it.
      final String qualifiedTableName;
      final String tableName = tableNameMapper.apply(resourceType.toCode());
      if (schema == null) {
        qualifiedTableName = tableName;
      } else {
        qualifiedTableName = String.join(".", schema, tableName);
      }

      // Read the table from the catalog.
      final Dataset<Row> table = pathlingContext.getSpark().read()
          .table(qualifiedTableName);

      // Use the InMemoryDataSourceBuilder to build the data source using the dataset read from
      // the catalog.
      inMemoryDataSourceBuilder.withResource(resourceType, table);
    }
    
    return inMemoryDataSourceBuilder.build();
  }

}
