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

package au.csiro.pathling.library.io.sink;

import static au.csiro.pathling.library.io.sink.DeltaSink.merge;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
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
  @Nonnull
  public WriteDetails write(@Nonnull final DataSource source) {
    final List<FileInformation> fileInfos = new ArrayList<>();
    for (final String resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String tableName = getTableName(resourceType);

      fileInfos.add(new FileInformation(resourceType, tableName));

      switch (saveMode) {
        case ERROR_IF_EXISTS, IGNORE -> writeDataset(dataset, tableName, saveMode);
        case APPEND -> append(dataset, tableName);
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
            merge(context, resourceType, table, dataset);
          } else {
            // If the table does not exist, create it.
            writeDataset(dataset, tableName, SaveMode.ERROR_IF_EXISTS);
          }
        }
        default -> throw new IllegalStateException("Unexpected save mode: " + saveMode);
      }
    }
    return new WriteDetails(fileInfos);
  }

  /**
   * Appends to a table, widening it where it is a Delta table and refusing a batch whose schema
   * differs from it where it is not (decision 86, FR-042).
   *
   * <p>A Delta table is widened to admit fields only the batch carries, as every other write into
   * an existing Delta table is. A table in another format cannot be: Spark refuses a differing
   * append at every level, narrower included, and there is no public route to add a nested field to
   * its stored schema. Such an append is refused here instead, before anything is written, with a
   * message naming the difference and the remedy. Field order is not a difference, because an
   * append places fields by name.
   *
   * @param dataset the batch to append
   * @param tableName the name of the table to append to
   * @throws IllegalArgumentException if the table exists, is not a Delta table, and its schema
   *     differs from the batch's
   */
  private void append(@Nonnull final Dataset<Row> dataset, @Nonnull final String tableName) {
    if (context.getSpark().catalog().tableExists(tableName)) {
      if (deltaTableExists(tableName)) {
        writeDataset(dataset, tableName, SaveMode.APPEND, true);
        return;
      }
      final List<String> differences =
          SchemaDifferences.between(context.getSpark().table(tableName).schema(), dataset.schema());
      if (!differences.isEmpty()) {
        throw new IllegalArgumentException(
            "Cannot append to table "
                + tableName
                + ", which is not a Delta table, because the schema of the data differs from the"
                + " table's: "
                + String.join("; ", differences)
                + ". Only a Delta table can be widened to admit the data. Write to a Delta table"
                + " instead, or write to a path and read it with the Parquet source, which"
                + " merges the schemas of its files.");
      }
    }
    writeDataset(dataset, tableName, SaveMode.APPEND);
  }

  private void writeDataset(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String tableName,
      @Nonnull final SaveMode saveMode) {
    writeDataset(dataset, tableName, saveMode, false);
  }

  private void writeDataset(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String tableName,
      @Nonnull final SaveMode saveMode,
      final boolean mergeSchema) {
    final DataFrameWriter<Row> writer = dataset.write();

    // Widen an existing Delta table to admit the fields only the data carries.
    if (mergeSchema) {
      writer.option("mergeSchema", "true");
    }

    // Apply save mode if it has a Spark equivalent.
    saveMode.getSparkSaveMode().ifPresent(writer::mode);

    // Apply format if specified.
    format.ifPresent(writer::format);

    writer.saveAsTable(tableName);
  }

  /**
   * Gets the table name for the given resource type.
   *
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
