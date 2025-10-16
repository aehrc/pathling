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

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
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
   * The (fallback) save mode to use when writing data when there is no entry in the perResource map.
   */
  @Nonnull
  private final SaveMode saveMode;

  /**
   * The save modes to use for each individual resource type when writing data.
   */
  @Nonnull
  private final Map<String, SaveMode> saveModePerResource;

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
      @Nonnull final Map<String, SaveMode> saveModePerResource,
      @Nonnull final UnaryOperator<String> fileNameMapper
  ) {
    this.context = context;
    this.path = path;
    this.saveMode = saveMode;
    this.saveModePerResource = saveModePerResource;
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
    this(context, path, saveMode, Map.of(), UnaryOperator.identity());
  }
  
  NdjsonSink(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final Map<String, SaveMode> saveModePerResource) {
    
  }

  @Override
  @Nonnull
  public NdjsonWriteDetails write(@Nonnull final DataSource source) {
    List<FileInfo> fileInfos = new ArrayList<>();
    for (final String resourceType : source.getResourceTypes()) {
      // Convert the dataset of structured FHIR data to a dataset of JSON strings.
      final Dataset<String> jsonStrings = context.decode(source.read(resourceType),
          resourceType, PathlingContext.FHIR_JSON);
      
      // Write the JSON strings to the file system. Each partition will have their id added to the name later
      final String fileName = String.join(".", fileNameMapper.apply(resourceType),
          "ndjson");
      final String resultUrl = safelyJoinPaths(path, fileName);
      
      switch (saveModePerResource.getOrDefault(resourceType, saveMode)) {
        case ERROR_IF_EXISTS, OVERWRITE, APPEND, IGNORE ->
            writeJsonStrings(jsonStrings, resultUrl, saveMode);
        case MERGE -> throw new UnsupportedOperationException(
            "Merge operation is not supported for NDJSON");
      }
      // Remove the partitioned directory and replace it with the renamed partitioned files
      // <resource_type>.<partId>.ndjson, i.e. Patient.00000.ndjson
      Collection<String> renamed = FileSystemPersistence.renamePartitionedFiles(context.getSpark(), resultUrl, resultUrl, "txt");
      renamed.forEach(renamedFilename -> fileInfos.add(new FileInfo(resourceType, renamedFilename, null)));
    }
    return new NdjsonWriteDetails(fileInfos);
  }

  void writeJsonStrings(@Nonnull final Dataset<String> jsonStrings,
      @Nonnull final String resultUrlPartitioned,
      @Nonnull final SaveMode saveMode) {
    //final var writer = jsonStrings.coalesce(1).write()
    final var writer = jsonStrings.write();
    // Apply save mode if it has a Spark equivalent
    saveMode.getSparkSaveMode().ifPresent(writer::mode);
    
    writer.text(resultUrlPartitioned);
  }

}
