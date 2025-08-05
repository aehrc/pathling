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

package au.csiro.pathling.library.io.sink;

import au.csiro.pathling.io.ImportMode;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * A data sink that writes data to a managed table within the Spark catalog.
 *
 * @author John Grimes
 */
public class CatalogSink implements DataSink {

  // TODO: Review whether we need to pass in the PathlingContext.
  @Nonnull
  private final PathlingContext pathlingContext;

  @Nonnull
  private final ImportMode importMode;

  @Nonnull
  private final Optional<String> schema;

  /**
   * Constructs a CatalogSink with the specified PathlingContext and default import mode.
   *
   * @param context the PathlingContext to use
   */
  public CatalogSink(@Nonnull final PathlingContext context) {
    this.pathlingContext = context;
    this.importMode = ImportMode.OVERWRITE; // Default import mode
    this.schema = Optional.empty(); // Schema not specified
  }

  /**
   * Constructs a CatalogSink with the specified PathlingContext and import mode.
   *
   * @param context the PathlingContext to use
   * @param importMode the ImportMode to use when writing data
   */
  public CatalogSink(@Nonnull final PathlingContext context, @Nonnull final ImportMode importMode) {
    this.pathlingContext = context;
    this.importMode = importMode;
    this.schema = Optional.empty(); // Schema not specified
  }

  /**
   * Constructs a CatalogSink with the specified PathlingContext, import mode, and schema.
   *
   * @param context the PathlingContext to use
   * @param importMode the ImportMode to use when writing data
   * @param schema the schema to qualify the table names, if any
   */
  public CatalogSink(@Nonnull final PathlingContext context, @Nonnull final ImportMode importMode,
      @Nonnull final String schema) {
    this.pathlingContext = context;
    this.importMode = importMode;
    this.schema = Optional.of(schema);
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final String resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String tableName = getTableName(resourceType);
      dataset.orderBy(functions.asc("id"))
          .write()
          .mode(importMode.getCode())
          .saveAsTable(tableName);
    }
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

}
