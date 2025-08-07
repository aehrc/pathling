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
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data sink that writes data to Parquet tables on a filesystem.
 *
 * @param path the path to write the Parquet files to
 * @param saveMode the {@link SaveMode} to use
 * @param fileNameMapper a function that maps resource type to file name
 * @author John Grimes
 */
public record ParquetSink(
    @Nonnull String path,
    @Nonnull SaveMode saveMode,
    @Nonnull UnaryOperator<String> fileNameMapper
) implements DataSink {

  /**
   * @param path the path to write the Parquet files to
   * @param saveMode the {@link SaveMode} to use
   */
  public ParquetSink(@Nonnull final String path, @Nonnull final SaveMode saveMode) {
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
        case ERROR_IF_EXISTS, OVERWRITE, APPEND, IGNORE ->
            writeDataset(dataset, tablePath, saveMode);
        case MERGE -> throw new UnsupportedOperationException(
            "Merge operation is not supported for Parquet - use Delta if merging is required");
      }
    }
  }

  private static void writeDataset(@Nonnull final Dataset<Row> dataset,
      @Nonnull final String tablePath, @Nonnull final SaveMode saveMode) {
    final var writer = dataset.write();
    
    // Apply save mode if it has a Spark equivalent
    saveMode.getSparkSaveMode().ifPresent(writer::mode);
    
    writer.parquet(tablePath);
  }

}
