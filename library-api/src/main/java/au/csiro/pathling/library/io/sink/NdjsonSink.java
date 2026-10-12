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

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirWriter;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data sink that writes data to NDJSON files on a filesystem.
 *
 * @author John Grimes
 */
final class NdjsonSink implements DataSink {

  /** The extension the JSON writer gives the files of each partition. */
  @Nonnull private static final String JSON_EXTENSION = "json";

  /** The Pathling context to use. */
  @Nonnull private final PathlingContext context;

  /** The path to write the NDJSON files to. */
  @Nonnull private final String path;

  /**
   * The (fallback) save mode to use when writing data when there is no entry in the perResource
   * map.
   */
  @Nonnull private final SaveMode saveMode;

  /** A function that maps resource type to file name. */
  @Nonnull private final UnaryOperator<String> fileNameMapper;

  /** Writes the stored resources as FHIR JSON. */
  @Nonnull private final FhirWriter writer;

  /**
   * Constructs an NdjsonSink with a custom file name mapper.
   *
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the NDJSON files to
   * @param saveMode the {@link SaveMode} to use
   * @param fileNameMapper a function that maps resource type to file name
   */
  NdjsonSink(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final SaveMode saveMode,
      @Nonnull final UnaryOperator<String> fileNameMapper) {
    this.context = context;
    this.path = path;
    this.saveMode = saveMode;
    this.fileNameMapper = fileNameMapper;
    this.writer = FhirWriter.of(FhirDefinitionContext.of(context.getFhirContext()));
  }

  /**
   * Constructs an NdjsonSink with default file naming.
   *
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the NDJSON files to
   * @param saveMode the {@link SaveMode} to use
   */
  NdjsonSink(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final SaveMode saveMode) {
    // By default, name the files using the resource type alone.
    this(context, path, saveMode, UnaryOperator.identity());
  }

  /**
   * Writes every resource type of the source to NDJSON files.
   *
   * <p>Every resource type is read before anything is written, so that a source refusing one of its
   * types, as one holding data in the layout written by earlier releases does, leaves nothing
   * written rather than some types and not others.
   *
   * @param source the data to write
   * @return the files written
   * @throws au.csiro.pathling.library.io.source.UnsupportedLayoutException if the source refuses
   *     its own data
   */
  @Override
  @Nonnull
  public WriteDetails write(@Nonnull final DataSource source) {
    final Map<String, Dataset<Row>> datasets = new LinkedHashMap<>();
    source.getResourceTypes().forEach(type -> datasets.put(type, source.read(type)));

    final List<FileInformation> fileInfos = new ArrayList<>();
    for (final Map.Entry<String, Dataset<Row>> entry : datasets.entrySet()) {
      final String resourceType = entry.getKey();
      // Write the resources to the file system as FHIR JSON. Each partition will have its id added
      // to the name later.
      final String fileName = String.join(".", fileNameMapper.apply(resourceType), "ndjson");
      final String resultUrl = safelyJoinPaths(path, fileName);

      switch (saveMode) {
        case ERROR_IF_EXISTS, OVERWRITE, APPEND, IGNORE ->
            writer
                .json()
                .write(
                    resourceType,
                    entry.getValue(),
                    resultUrl,
                    saveMode.getSparkSaveMode().orElseThrow());
        case MERGE ->
            throw new UnsupportedOperationException("Merge operation is not supported for NDJSON");
        default -> throw new IllegalStateException("Unexpected save mode: " + saveMode);
      }
      // Remove the partitioned directory and replace it with the renamed partitioned files
      // <resource_type>.<partId>.ndjson, i.e. Patient.00000.ndjson
      final Collection<String> renamed =
          FileSystemPersistence.renamePartitionedFiles(
              context.getSpark(), resultUrl, resultUrl, JSON_EXTENSION);
      renamed.forEach(
          renamedFilename -> fileInfos.add(new FileInformation(resourceType, renamedFilename)));
    }
    return new WriteDetails(fileInfos);
  }
}
