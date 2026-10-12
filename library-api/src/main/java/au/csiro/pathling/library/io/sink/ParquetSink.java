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
import au.csiro.pathling.library.io.FileSystemPersistence;
import au.csiro.pathling.library.io.PersistenceError;
import au.csiro.pathling.library.io.SaveMode;
import au.csiro.pathling.library.io.source.FileSource;
import au.csiro.pathling.library.io.source.LayoutDetector;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data sink that writes data to Parquet tables on a filesystem.
 *
 * @author John Grimes
 */
final class ParquetSink implements DataSink {

  /** The extension of the files and tables this sink writes. */
  @Nonnull private static final String PARQUET_EXTENSION = "parquet";

  /** The Pathling context to use. */
  @Nonnull private final PathlingContext context;

  /** The path to write the Parquet files to. */
  @Nonnull private final String path;

  /** The save mode to use when writing data. */
  @Nonnull private final SaveMode saveMode;

  /** A function that maps resource type to file name. */
  @Nonnull private final UnaryOperator<String> fileNameMapper;

  /**
   * Constructs a ParquetSink with a custom file name mapper.
   *
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the Parquet files to
   * @param saveMode the {@link SaveMode} to use
   * @param fileNameMapper a function that maps resource type to file name
   */
  ParquetSink(
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
   * Constructs a ParquetSink with default file naming.
   *
   * @param context the {@link PathlingContext} to use
   * @param path the path to write the Parquet files to
   * @param saveMode the {@link SaveMode} to use
   */
  ParquetSink(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final SaveMode saveMode) {
    // By default, name the files using the resource type alone.
    this(context, path, saveMode, UnaryOperator.identity());
  }

  /**
   * Writes every resource type of the source to its files.
   *
   * <p>Every resource type is read, and, for an append, the files each type already has in the
   * directory are classified, before anything is written, so that a refusal of either kind leaves
   * the directory as it was rather than some types written and some not (FR-037a).
   *
   * @param source the data to write
   * @return the files written
   * @throws au.csiro.pathling.library.io.source.UnsupportedLayoutException if the files an append
   *     would add to are in a layout this release does not read, or the source refuses its own data
   */
  @Override
  @Nonnull
  public WriteDetails write(@Nonnull final DataSource source) {
    final Map<String, Dataset<Row>> datasets = new LinkedHashMap<>();
    source.getResourceTypes().forEach(type -> datasets.put(type, source.read(type)));
    if (saveMode == SaveMode.APPEND) {
      datasets.keySet().forEach(this::checkTargetLayout);
    }

    final List<FileInformation> fileInfos = new ArrayList<>();
    for (final Map.Entry<String, Dataset<Row>> entry : datasets.entrySet()) {
      final String resourceType = entry.getKey();
      final Dataset<Row> dataset = entry.getValue();
      final String fileName =
          String.join(".", fileNameMapper.apply(resourceType), PARQUET_EXTENSION);
      final String tablePath = safelyJoinPaths(path, fileName);

      switch (saveMode) {
        case ERROR_IF_EXISTS, OVERWRITE, APPEND, IGNORE ->
            writeDataset(dataset, tablePath, saveMode);
        case MERGE ->
            throw new UnsupportedOperationException(
                "Merge operation is not supported for Parquet - use Delta if merging is required");
        default -> throw new IllegalStateException("Unexpected save mode: " + saveMode);
      }

      // Remove the partitioned directory and replace it with the renamed partitioned files.
      // Files follow the pattern {resourceType}.{partId}.parquet, e.g. Patient.00000.parquet.
      final Collection<String> renamed =
          FileSystemPersistence.renamePartitionedFiles(
              context.getSpark(), tablePath, tablePath, PARQUET_EXTENSION);
      renamed.forEach(
          renamedFilename -> fileInfos.add(new FileInformation(resourceType, renamedFilename)));
    }
    return new WriteDetails(fileInfos);
  }

  /**
   * Refuses an append beside existing files in a layout this release does not read, which would
   * leave the directory holding both layouts. The existing files are those the Parquet source reads
   * as the type: the table the type is named for, and the partition files renamed from it. Their
   * schemas are merged to classify them, as the source merges them to read them.
   *
   * @param resourceType the resource type whose existing files are checked
   */
  private void checkTargetLayout(@Nonnull final String resourceType) {
    final String fileName = fileNameMapper.apply(resourceType);
    final Path directory = new Path(path);
    final FileSystem fileSystem = FileSystemPersistence.getFileSystem(context.getSpark(), path);
    final String[] existing;
    try {
      if (!fileSystem.exists(directory)) {
        return;
      }
      existing =
          Arrays.stream(fileSystem.listStatus(directory))
              .map(FileStatus::getPath)
              .filter(file -> FilenameUtils.isExtension(file.getName(), PARQUET_EXTENSION))
              .filter(
                  file ->
                      FileSource.resourceNameWithQualifierMapper(
                              FilenameUtils.getBaseName(file.getName()))
                          .contains(fileName))
              .map(Path::toString)
              .toArray(String[]::new);
    } catch (final IOException e) {
      throw new PersistenceError("Problem listing the files of " + resourceType + " in " + path, e);
    }
    if (existing.length > 0) {
      LayoutDetector.check(
          resourceType,
          context.getSpark().read().option("mergeSchema", "true").parquet(existing).schema());
    }
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
