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

import static au.csiro.pathling.io.FileSystemPersistence.departitionResult;
import static au.csiro.pathling.io.FileSystemPersistence.safelyJoinPaths;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.FhirMimeTypes;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

/**
 * A data sink that writes data to NDJSON files on a filesystem.
 *
 * @author John Grimes
 */
public class NdjsonSink implements DataSink {

  @Nonnull
  private final PathlingContext context;

  @Nonnull
  private final String path;

  @Nonnull
  private final SaveMode saveMode;

  @Nonnull
  private final UnaryOperator<String> fileNameMapper;

  /**
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the NDJSON files to
   * @param saveMode the {@link SaveMode} to use
   */
  public NdjsonSink(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final SaveMode saveMode) {
    this(context, path, saveMode, UnaryOperator.identity());
  }

  /**
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the NDJSON files to
   * @param saveMode the {@link SaveMode} to use
   * @param fileNameMapper a function that maps resource type to file name
   */
  public NdjsonSink(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final SaveMode saveMode, @Nonnull final UnaryOperator<String> fileNameMapper) {
    this.context = context;
    this.path = path;
    this.saveMode = saveMode;
    this.fileNameMapper = fileNameMapper;
  }

  @Override
  public void write(@Nonnull final DataSource source) {
    for (final String resourceType : source.getResourceTypes()) {
      // Convert the dataset of structured FHIR data to a dataset of JSON strings.
      final Dataset<String> jsonStrings = context.decode(source.read(resourceType),
          resourceType, FhirMimeTypes.FHIR_JSON);

      // Write the JSON strings to the file system, using a single partition.
      final String fileName = String.join(".", fileNameMapper.apply(resourceType),
          "ndjson");
      final String resultUrl = safelyJoinPaths(path, fileName);
      final String resultUrlPartitioned = resultUrl + ".partitioned";
      jsonStrings.coalesce(1).write().mode(saveMode).text(resultUrlPartitioned);

      // Remove the partitioned directory and replace it with a single file.
      departitionResult(context.getSpark(), resultUrlPartitioned, resultUrl, "txt");
    }
  }

}
