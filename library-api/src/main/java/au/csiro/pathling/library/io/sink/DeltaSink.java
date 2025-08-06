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
import au.csiro.pathling.library.io.ImportMode;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * A data sink that writes data to a Delta Lake table on a filesystem.
 *
 * @author John Grimes
 */
public class DeltaSink implements DataSink {

  @Nonnull
  private final ImportMode importMode;

  @Nonnull
  private final String path;

  /**
   * @param path the path to write the Delta database to
   */
  public DeltaSink(@Nonnull final String path) {
    this.importMode = ImportMode.ERROR_IF_EXISTS; // Default import mode
    this.path = path;
  }

  /**
   * @param path the path to write the Delta database to
   * @param importMode the {@link ImportMode} to use
   */
  public DeltaSink(@Nonnull final String path, @Nonnull final ImportMode importMode) {
    this.importMode = importMode;
    this.path = path;
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final String resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String tablePath = safelyJoinPaths(path, resourceType + ".parquet");

      switch (importMode) {
        case ERROR_IF_EXISTS -> writeDataset(dataset, tablePath, SaveMode.ErrorIfExists);
        case OVERWRITE -> writeDataset(dataset, tablePath, SaveMode.Overwrite);
        case APPEND -> writeDataset(dataset, tablePath, SaveMode.Append);
        case MERGE -> {
          if (DeltaTable.isDeltaTable(tablePath)) {
            // If the table already exists, merge the data in.
            final DeltaTable table = DeltaTable.forPath(tablePath);
            merge(table, dataset);
          } else {
            // If the table does not exist, create it. If an error occurs here, there must be a
            // pre-existing file at the path that is not a Delta table.
            writeDataset(dataset, tablePath, SaveMode.ErrorIfExists);
          }
        }
      }
    }
  }

  /**
   * Writes the data to a Delta table at the specified path with the specified save mode.
   *
   * @param saveMode the save mode to use for writing
   * @param dataset the dataset to write to the Delta table
   */
  private static void writeDataset(@Nonnull final Dataset<Row> dataset,
      @Nonnull final String tablePath, final SaveMode saveMode) {
    dataset.write()
        .format("delta")
        .mode(saveMode)
        .save(tablePath);
  }

  /**
   * Merges the given dataset into the specified Delta table.
   *
   * @param table the Delta table to merge into
   * @param dataset the dataset containing updates to be merged
   */
  static void merge(@Nonnull final DeltaTable table, @Nonnull final Dataset<Row> dataset) {
    table.as("original")
        .merge(dataset.as("updates"), "original.id = updates.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute();
  }

}
