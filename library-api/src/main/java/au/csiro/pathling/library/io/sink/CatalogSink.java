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

package au.csiro.pathling.library.io.sink;

import static au.csiro.pathling.library.io.sink.DeltaSink.merge;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data sink that writes data to a managed table within the Spark catalog.
 *
 * @author John Grimes
 */
class CatalogSink implements DataSink {

  @Nonnull private final PathlingContext context;

  @Nonnull private final SaveMode saveMode;

  @Nonnull private final Optional<String> schema;

  @Nonnull private final Optional<String> format;

  /**
   * Constructs a CatalogSink with the specified PathlingContext and default import mode.
   *
   * @param context the PathlingContext to use
   */
  CatalogSink(@Nonnull final PathlingContext context) {
    this.context = context;
    this.saveMode = SaveMode.ERROR_IF_EXISTS; // Default import mode
    this.schema = Optional.empty(); // Schema not specified
    this.format = Optional.empty(); // Format not specified
  }

  /**
   * Constructs a CatalogSink with the specified PathlingContext and import mode.
   *
   * @param context the PathlingContext to use
   * @param saveMode the SaveMode to use when writing data
   */
  CatalogSink(@Nonnull final PathlingContext context, @Nonnull final SaveMode saveMode) {
    this.context = context;
    this.saveMode = saveMode;
    this.schema = Optional.empty(); // Schema not specified
    this.format = Optional.empty(); // Format not specified
  }

  /**
   * Constructs a CatalogSink with the specified PathlingContext, import mode, and schema.
   *
   * @param context the PathlingContext to use
   * @param saveMode the SaveMode to use when writing data
   * @param schema the schema to qualify the table names, if any
   */
  CatalogSink(
      @Nonnull final PathlingContext context,
      @Nonnull final SaveMode saveMode,
      @Nonnull final String schema) {
    this.context = context;
    this.saveMode = saveMode;
    this.schema = Optional.of(schema);
    this.format = Optional.empty(); // Format not specified
  }

  /**
   * Constructs a CatalogSink with the specified PathlingContext, import mode, schema, and format.
   *
   * @param context the PathlingContext to use
   * @param saveMode the SaveMode to use when writing data
   * @param schema the schema to qualify the table names, if any
   * @param format the format to use when writing data
   */
  CatalogSink(
      @Nonnull final PathlingContext context,
      @Nonnull final SaveMode saveMode,
      @Nonnull final String schema,
      @Nonnull final String format) {
    this.context = context;
    this.saveMode = saveMode;
    this.schema = Optional.of(schema);
    this.format = Optional.of(format);
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final String resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String tableName = getTableName(resourceType);

      switch (saveMode) {
        case ERROR_IF_EXISTS, APPEND, IGNORE -> writeDataset(dataset, tableName, saveMode);
        case OVERWRITE -> {
          if (format.isPresent() && "delta".equals(format.get())) {
            // This is to work around a bug relating to Delta tables not being able to be
            // overwritten,
            // due to their inability to handle the truncate operation that Spark performs when
            // overwriting a table.
            context.getSpark().sql("DROP TABLE IF EXISTS " + tableName);
            writeDataset(dataset, tableName, SaveMode.ERROR_IF_EXISTS);
          } else {
            // Use standard overwrite for non-Delta formats.
            writeDataset(dataset, tableName, saveMode);
          }
        }
        case MERGE -> {
          if (deltaTableExists(tableName)) {
            // If the table already exists, merge the data in.
            final DeltaTable table = DeltaTable.forName(tableName);
            merge(table, dataset);
          } else {
            // If the table does not exist, create it.
            writeDataset(dataset, tableName, SaveMode.ERROR_IF_EXISTS);
          }
        }
      }
    }
  }

  private void writeDataset(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String tableName,
      @Nonnull final SaveMode saveMode) {
    final DataFrameWriter<Row> writer = dataset.write();

    // Apply save mode if it has a Spark equivalent.
    saveMode.getSparkSaveMode().ifPresent(writer::mode);

    // Apply format if specified.
    format.ifPresent(writer::format);

    writer.saveAsTable(tableName);
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
   * Checks if a Delta table exists by attempting to access it via DeltaTable.forName. This method
   * catches any exceptions that occur during table access to determine existence.
   *
   * @param tableName the name of the table to check
   * @return true if the Delta table exists, false otherwise
   */
  private boolean deltaTableExists(@Nonnull final String tableName) {
    try {
      DeltaTable.forName(tableName);
      return true;
    } catch (final Exception e) {
      // Table does not exist or is not a Delta table.
      return false;
    }
  }
}
