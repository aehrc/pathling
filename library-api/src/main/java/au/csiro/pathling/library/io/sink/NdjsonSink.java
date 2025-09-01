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

import static au.csiro.pathling.library.io.FileSystemPersistence.departitionResult;
import static au.csiro.pathling.library.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;

/**
 * A data sink that writes data to NDJSON files on a filesystem.
 *
 * @author John Grimes
 */
final class NdjsonSink implements DataSink {

  /**
   * The Pathling context to use.
   */
  @Nonnull
  private final PathlingContext context;

  /**
   * The path to write the NDJSON files to.
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
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the NDJSON files to
   * @param saveMode the {@link SaveMode} to use
   * @param fileNameMapper a function that maps resource type to file name
   */
  NdjsonSink(
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
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the NDJSON files to
   * @param saveMode the {@link SaveMode} to use
   */
  NdjsonSink(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final SaveMode saveMode) {
    // By default, name the files using the resource type alone.
    this(context, path, saveMode, UnaryOperator.identity());
  }

  @Override
  public NdjsonWriteDetails write(@Nonnull final DataSource source) {
    List<FileInfo> fileInfos = new ArrayList<>();
    for (final String resourceType : source.getResourceTypes()) {
      // Convert the dataset of structured FHIR data to a dataset of JSON strings.
      final Dataset<String> jsonStrings = context.decode(source.read(resourceType),
          resourceType, PathlingContext.FHIR_JSON);
      
      // Write the JSON strings to the file system, using a single partition.
      final String fileName = String.join(".", fileNameMapper.apply(resourceType),
          "ndjson");
      final String resultUrl = safelyJoinPaths(path, fileName);
      final String resultUrlPartitioned = resultUrl + ".partitioned";

      switch (saveMode) {
        case ERROR_IF_EXISTS, OVERWRITE, APPEND, IGNORE ->
            writeJsonStrings(jsonStrings, resultUrlPartitioned, saveMode);
        case MERGE -> throw new UnsupportedOperationException(
            "Merge operation is not supported for NDJSON");
      }
      FileInfo fileInfo = new FileInfo(resourceType, resultUrl, jsonStrings.count());
      fileInfos.add(fileInfo);
      // Remove the partitioned directory and replace it with a single file.
      departitionResult(context.getSpark(), resultUrlPartitioned, resultUrl, "txt");
    }
    return new NdjsonWriteDetails(fileInfos);
  }

  void writeJsonStrings(@Nonnull final Dataset<String> jsonStrings,
      @Nonnull final String resultUrlPartitioned,
      @Nonnull final SaveMode saveMode) {
    final var writer = jsonStrings.coalesce(1)
        .write();
    // Apply save mode if it has a Spark equivalent
    saveMode.getSparkSaveMode().ifPresent(writer::mode);
    
    writer.text(resultUrlPartitioned);
  }

}
