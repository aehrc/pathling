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

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.SaveMode;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data sink that writes data to a Delta Lake table on a filesystem.
 *
 * @param path the path to write the Delta database to
 * @param saveMode the {@link SaveMode} to use
 * @param fileNameMapper a function that maps resource type to file name
 * @author John Grimes
 */
public record DeltaSink(
    @Nonnull String path,
    @Nonnull SaveMode saveMode,
    @Nonnull UnaryOperator<String> fileNameMapper
) implements DataSink {

  /**
   * @param path the path to write the Delta database to
   */
  public DeltaSink(@Nonnull final String path) {
    // By default, name the files using the resource type alone.
    this(path, SaveMode.ERROR_IF_EXISTS, UnaryOperator.identity());
  }

  /**
   * @param path the path to write the Delta database to
   * @param saveMode the {@link SaveMode} to use
   */
  public DeltaSink(@Nonnull final String path, @Nonnull final SaveMode saveMode) {
    // By default, name the files using the resource type alone.
    this(path, saveMode, UnaryOperator.identity());
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final String resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String fileName = String.join(".", fileNameMapper.apply(resourceType),
          "parquet");
      final String tablePath = safelyJoinPaths(path, fileName);

      switch (saveMode) {
        case ERROR_IF_EXISTS, APPEND, IGNORE ->
            writeDataset(dataset, tablePath, saveMode);
        case OVERWRITE -> {
          // This is to work around a bug relating to Delta tables not being able to be overwritten,
          // due to their inability to handle the truncate operation that Spark performs when
          // overwriting a table.
          if (deltaTableExists(tablePath)) {
            final DeltaTable table = DeltaTable.forPath(tablePath);
            table.delete();
          }
          writeDataset(dataset, tablePath, SaveMode.ERROR_IF_EXISTS);
        }
        case MERGE -> {
          if (deltaTableExists(tablePath)) {
            // If the table already exists, merge the data in.
            final DeltaTable table = DeltaTable.forPath(tablePath);
            merge(table, dataset);
          } else {
            // If the table does not exist, create it. If an error occurs here, there must be a
            // pre-existing file at the path that is not a Delta table.
            writeDataset(dataset, tablePath, SaveMode.ERROR_IF_EXISTS);
          }
        }
      }
    }
  }

  /**
   * Writes the data to a Delta table at the specified path with the specified save mode.
   *
   * @param dataset the dataset to write to the Delta table
   * @param tablePath the path to write the Delta table to
   * @param saveMode the save mode to use for writing
   */
  private static void writeDataset(@Nonnull final Dataset<Row> dataset,
      @Nonnull final String tablePath, @Nonnull final SaveMode saveMode) {
    final var writer = dataset.write()
        .format("delta");
    
    // Apply save mode if it has a Spark equivalent
    saveMode.getSparkSaveMode().ifPresent(writer::mode);
    
    writer.save(tablePath);
  }

  /**
   * Merges the given dataset into the specified Delta table.
   *
   * @param table the Delta table to merge into
   * @param dataset the dataset containing updates to be merged
   */
  static void merge(@Nonnull final DeltaTable table, @Nonnull final Dataset<Row> dataset) {
    // Perform a merge operation where we match on the 'id' column.
    table.as("original")
        .merge(dataset.as("updates"), "original.id = updates.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute();
  }

  /**
   * Checks if a Delta table exists at the specified path by attempting to access it via
   * DeltaTable.forPath. This method catches any exceptions that occur during table access
   * to determine existence.
   *
   * @param tablePath the path to the table to check
   * @return true if the Delta table exists, false otherwise
   */
  private static boolean deltaTableExists(@Nonnull final String tablePath) {
    try {
      DeltaTable.forPath(tablePath);
      return true;
    } catch (final Exception e) {
      // Table does not exist or is not a Delta table.
      return false;
    }
  }

}
