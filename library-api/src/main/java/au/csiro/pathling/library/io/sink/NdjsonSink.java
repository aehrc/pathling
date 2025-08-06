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

import static au.csiro.pathling.library.io.FileSystemPersistence.departitionResult;
import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.ImportMode;
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

/**
 * A data sink that writes data to NDJSON files on a filesystem.
 *
 * @param context the {@link PathlingContext} to use
 * @param path the path to write the NDJSON files to
 * @param importMode the {@link ImportMode} to use
 * @param fileNameMapper a function that maps resource type to file name
 * @author John Grimes
 */
public record NdjsonSink(
    @Nonnull PathlingContext context,
    @Nonnull String path,
    @Nonnull ImportMode importMode,
    @Nonnull UnaryOperator<String> fileNameMapper
) implements DataSink {

  /**
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the NDJSON files to
   * @param importMode the {@link ImportMode} to use
   */
  public NdjsonSink(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final ImportMode importMode) {
    this(context, path, importMode, UnaryOperator.identity());
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final String resourceType : source.getResourceTypes()) {
      // Convert the dataset of structured FHIR data to a dataset of JSON strings.
      final Dataset<String> jsonStrings = context.decode(source.read(resourceType),
          resourceType, PathlingContext.FHIR_JSON);

      // Write the JSON strings to the file system, using a single partition.
      final String fileName = String.join(".", fileNameMapper.apply(resourceType),
          "ndjson");
      final String resultUrl = safelyJoinPaths(path, fileName);
      final String resultUrlPartitioned = resultUrl + ".partitioned";

      switch (importMode) {
        case ERROR_IF_EXISTS ->
            writeJsonStrings(jsonStrings, resultUrlPartitioned, SaveMode.ErrorIfExists);
        case OVERWRITE -> writeJsonStrings(jsonStrings, resultUrlPartitioned, SaveMode.Overwrite);
        case APPEND -> writeJsonStrings(jsonStrings, resultUrlPartitioned, SaveMode.Append);
        case MERGE -> throw new UnsupportedOperationException(
            "Merge operation is not supported for NDJSON");
      }

      // Remove the partitioned directory and replace it with a single file.
      departitionResult(context.getSpark(), resultUrlPartitioned, resultUrl, "txt");
    }
  }

  private static void writeJsonStrings(@Nonnull final Dataset<String> jsonStrings,
      @Nonnull final String resultUrlPartitioned, @Nonnull final SaveMode saveMode) {
    jsonStrings.coalesce(1)
        .write()
        .mode(saveMode)
        .text(resultUrlPartitioned);
  }

}
