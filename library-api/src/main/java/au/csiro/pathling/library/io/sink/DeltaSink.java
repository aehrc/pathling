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

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.PersistenceError;
import au.csiro.pathling.library.io.SaveMode;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data sink that writes data to a Delta Lake table on a filesystem.
 *
 * @author John Grimes
 */
final class DeltaSink implements DataSink {

  /**
   * The Pathling context to use.
   */
  @Nonnull
  private final PathlingContext context;

  /**
   * The path to write the Delta database to.
   */
  @Nonnull
  private final String path;

  /**
   * The save mode to use when writing data.
   */
  @Nonnull
  private final SaveMode saveMode;

  /**
   * A function that maps resource type to file name.
   */
  @Nonnull
  private final UnaryOperator<String> fileNameMapper;

  /**
   * @param context the PathlingContext to use
   * @param path the path to write the Delta database to
   * @param saveMode the {@link SaveMode} to use
   * @param fileNameMapper a function that maps resource type to file name
   *
   */
  DeltaSink(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final SaveMode saveMode,
      @Nonnull final UnaryOperator<String> fileNameMapper
  ) {
    this.context = context;
    this.path = path;
    this.saveMode = saveMode;
    this.fileNameMapper = fileNameMapper;
  }

  /**
   * @param context the PathlingContext to use
   * @param path the path to write the Delta database to
   */
  DeltaSink(@Nonnull final PathlingContext context, @Nonnull final String path) {
    // By default, name the files using the resource type alone.
    this(context, path, SaveMode.ERROR_IF_EXISTS, UnaryOperator.identity());
  }

  /**
   * @param context the PathlingContext to use
   * @param path the path to write the Delta database to
   * @param saveMode the {@link SaveMode} to use
   */
  DeltaSink(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final SaveMode saveMode) {
    // By default, name the files using the resource type alone.
    this(context, path, saveMode, UnaryOperator.identity());
  }

  @Override
  @Nonnull
  public WriteDetails write(@Nonnull final DataSource source) {
    List<FileInfo> fileInfos = new ArrayList<>();
    for (final String resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String fileName = String.join(".", fileNameMapper.apply(resourceType),
          "parquet");
      final String tablePath = safelyJoinPaths(path, fileName);
      
      fileInfos.add(new FileInfo(resourceType, tablePath, null));

      switch (saveMode) {
        case ERROR_IF_EXISTS, APPEND, IGNORE -> writeDataset(dataset, tablePath, saveMode);
        case OVERWRITE -> {
          // This is to work around a bug relating to Delta tables not being able to be overwritten,
          // due to their inability to handle the truncate operation that Spark performs when
          // overwriting a table.
          if (deltaTableExists(tablePath)) {
            delete(tablePath);
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
    return new WriteDetails(fileInfos);
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
   * DeltaTable.forPath. This method catches any exceptions that occur during table access to
   * determine existence.
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

  /**
   * Deletes the Delta table at the specified URL.
   *
   * @param tableUrl the URL of the Delta table to delete
   */
  private void delete(@Nonnull final String tableUrl) {
    try {
      final Path tablePath = new Path(tableUrl);
      final FileSystem fileSystem = getFileSystem(path);
      if (fileSystem.exists(tablePath)) {
        fileSystem.delete(tablePath, true);
      }
    } catch (final IOException e) {
      throw new PersistenceError("Failed to delete table: " + tableUrl, e);
    }
  }

  /**
   * Get a Hadoop {@link FileSystem} for the given location.
   *
   * @param location the location URL to be accessed
   * @return the {@link FileSystem} for the given location
   */
  @Nonnull
  private FileSystem getFileSystem(@Nonnull final String location) {
    @Nullable final Configuration hadoopConfiguration = context.getSpark()
        .sparkContext().hadoopConfiguration();
    requireNonNull(hadoopConfiguration);
    @Nullable final FileSystem warehouseLocation;
    try {
      warehouseLocation = FileSystem.get(new URI(location), hadoopConfiguration);
    } catch (final IOException e) {
      throw new PersistenceError("Problem accessing location: " + location, e);
    } catch (final URISyntaxException e) {
      throw new PersistenceError("Problem parsing URL: " + location, e);
    }
    requireNonNull(warehouseLocation);
    return warehouseLocation;
  }

}
