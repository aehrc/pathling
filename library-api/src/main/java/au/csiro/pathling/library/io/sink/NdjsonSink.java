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
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.PersistenceError;
import au.csiro.pathling.library.io.SaveMode;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data sink that writes data to NDJSON files on a filesystem.
 *
 * @author John Grimes
 */
final class NdjsonSink implements DataSink {

  /** The extension the text writer gives the files of each partition. */
  @Nonnull private static final String TEXT_EXTENSION = "txt";

  /** The extension of the files this sink writes. */
  @Nonnull private static final String NDJSON_EXTENSION = "ndjson";

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
   * <p>Every resource type is read, and the files each type already has in the directory are found,
   * before anything is written, so that a refusal by the save mode leaves the directory as it was
   * rather than some types written and some not. The save mode applies to those files: an append
   * adds beside them; an overwrite replaces them; an error-if-exists write is refused where there
   * are any; and an ignored write skips the type where there are any.
   *
   * @param source the data to write
   * @return the files written
   * @throws PersistenceError if the save mode is error-if-exists and a type already has files
   */
  @Override
  @Nonnull
  public WriteDetails write(@Nonnull final DataSource source) {
    final Map<String, Dataset<Row>> datasets = new LinkedHashMap<>();
    source.getResourceTypes().forEach(type -> datasets.put(type, source.read(type)));
    final Map<String, List<Path>> existing = new LinkedHashMap<>();
    datasets
        .keySet()
        .forEach(
            type ->
                existing.put(
                    type,
                    TypeFiles.list(
                        context.getSpark(), path, fileNameMapper.apply(type), NDJSON_EXTENSION)));
    if (saveMode == SaveMode.ERROR_IF_EXISTS) {
      existing.forEach((type, files) -> TypeFiles.refuseIfAny(type, files, path));
    }

    final List<FileInformation> fileInfos = new ArrayList<>();
    for (final Map.Entry<String, Dataset<Row>> entry : datasets.entrySet()) {
      final String resourceType = entry.getKey();
      final List<Path> replaced = existing.get(resourceType);
      if (saveMode == SaveMode.IGNORE && !replaced.isEmpty()) {
        continue;
      }
      // Convert the dataset of structured FHIR data to a dataset of JSON strings.
      final Dataset<String> jsonStrings =
          context.decode(entry.getValue(), resourceType, PathlingContext.FHIR_JSON);

      // Write the JSON strings to the file system. Each partition will have their id added to the
      // name later
      final String fileName =
          String.join(".", fileNameMapper.apply(resourceType), NDJSON_EXTENSION);
      final String resultUrl = safelyJoinPaths(path, fileName);

      switch (saveMode) {
        case ERROR_IF_EXISTS, OVERWRITE, APPEND, IGNORE ->
            writeJsonStrings(jsonStrings, resultUrl, saveMode);
        case MERGE ->
            throw new UnsupportedOperationException("Merge operation is not supported for NDJSON");
        default -> throw new IllegalStateException("Unexpected save mode: " + saveMode);
      }
      // Remove the partitioned directory and replace it with the renamed partitioned files
      // <resource_type>.<partId>-<writeId>.ndjson, e.g.
      // Patient.00000-7a60a041-0e7d-4e24-b23d-82405a665677.ndjson, so that a write never takes the
      // names of an earlier write's files.
      final List<String> renamed =
          TypeFiles.renameKeepingWriteId(context.getSpark(), resultUrl, TEXT_EXTENSION);
      if (saveMode == SaveMode.OVERWRITE) {
        TypeFiles.deleteReplaced(context.getSpark(), path, replaced, resultUrl);
      }
      renamed.forEach(
          renamedFilename -> fileInfos.add(new FileInformation(resourceType, renamedFilename)));
    }
    return new WriteDetails(fileInfos);
  }

  void writeJsonStrings(
      @Nonnull final Dataset<String> jsonStrings,
      @Nonnull final String resultUrlPartitioned,
      @Nonnull final SaveMode saveMode) {
    // final var writer = jsonStrings.coalesce(1).write()
    final var writer = jsonStrings.write();
    // Apply save mode if it has a Spark equivalent
    saveMode.getSparkSaveMode().ifPresent(writer::mode);

    writer.text(resultUrlPartitioned);
  }
}
