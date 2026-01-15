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

import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data sink that writes data to Parquet tables on a filesystem.
 *
 * @author John Grimes
 */
final class ParquetSink implements DataSink {

  /** The path to write the Parquet files to. */
  @Nonnull private final String path;

  /** The save mode to use when writing data. */
  @Nonnull private final SaveMode saveMode;

  /** A function that maps resource type to file name. */
  @Nonnull private final UnaryOperator<String> fileNameMapper;

  /**
   * Constructs a ParquetSink with a custom file name mapper.
   *
   * @param path the path to write the Parquet files to
   * @param saveMode the {@link SaveMode} to use
   * @param fileNameMapper a function that maps resource type to file name
   */
  ParquetSink(
      @Nonnull final String path,
      @Nonnull final SaveMode saveMode,
      @Nonnull final UnaryOperator<String> fileNameMapper) {
    this.path = path;
    this.saveMode = saveMode;
    this.fileNameMapper = fileNameMapper;
  }

  /**
   * Constructs a ParquetSink with default file naming.
   *
   * @param path the path to write the Parquet files to
   * @param saveMode the {@link SaveMode} to use
   */
  ParquetSink(@Nonnull final String path, @Nonnull final SaveMode saveMode) {
    // By default, name the files using the resource type alone.
    this(path, saveMode, UnaryOperator.identity());
  }

  @Override
  @Nonnull
  public WriteDetails write(@Nonnull final DataSource source) {
    final List<FileInformation> fileInfos = new ArrayList<>();
    for (final String resourceType : source.getResourceTypes()) {
      final Dataset<Row> dataset = source.read(resourceType);
      final String fileName = String.join(".", fileNameMapper.apply(resourceType), "parquet");
      final String tablePath = safelyJoinPaths(path, fileName);

      fileInfos.add(new FileInformation(resourceType, tablePath));

      switch (saveMode) {
        case ERROR_IF_EXISTS, OVERWRITE, APPEND, IGNORE ->
            writeDataset(dataset, tablePath, saveMode);
        case MERGE ->
            throw new UnsupportedOperationException(
                "Merge operation is not supported for Parquet - use Delta if merging is required");
        default -> throw new IllegalStateException("Unexpected save mode: " + saveMode);
      }
    }
    return new WriteDetails(fileInfos);
  }

  void writeDataset(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String tablePath,
      @Nonnull final SaveMode saveMode) {
    final var writer = dataset.write();

    // Apply save mode if it has a Spark equivalent
    saveMode.getSparkSaveMode().ifPresent(writer::mode);

    writer.parquet(tablePath);
  }
}
