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

package au.csiro.pathling.library.io.source;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import scala.collection.JavaConverters;

/**
 * Common functionality for file-based sources.
 *
 * @author John Grimes
 */
@Slf4j
public abstract class FileSource extends DatasetSource {

  /**
   * A function that maps a resource type code to a set of file names that contain data for that
   * resource type.
   */
  protected final Function<String, Set<String>> fileNameMapper;

  /**
   * The file extension that this source expects for its source files.
   */
  @Nonnull
  protected final String extension;

  /**
   * A {@link DataFrameReader} that can be used to read the source files.
   */
  @Nonnull
  protected final DataFrameReader reader;

  /**
   * A function that transforms a {@link Dataset<Row>} containing raw source data of a specified
   * resource type into a {@link Dataset<Row>} containing the imported data.
   */
  @Nonnull
  protected final BiFunction<Dataset<Row>, String, Dataset<Row>> transformer;

  /**
   * @param context the Pathling context
   * @param path the path to the source files, which may be a directory or a glob pattern
   * @param fileNameMapper a function that maps a file name to a set of resource types
   * @param extension the file extension that this source expects for its source files
   * @param reader a {@link DataFrameReader} that can be used to read the source files
   * @param transformer a function that transforms a {@link Dataset<Row>} containing raw source data
   * of a specified resource type into a {@link Dataset<Row>} containing the imported data
   */
  protected FileSource(@Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final Function<String, Set<String>> fileNameMapper, @Nonnull final String extension,
      @Nonnull final DataFrameReader reader,
      @Nonnull final BiFunction<Dataset<Row>, String, Dataset<Row>> transformer) {
    super(context);
    this.fileNameMapper = fileNameMapper;
    this.extension = extension;
    this.reader = reader;
    this.transformer = transformer;

    final org.apache.hadoop.conf.Configuration hadoopConfiguration = requireNonNull(
        context.getSpark().sparkContext().hadoopConfiguration());
    try {
      // If the URL is an S3 URL, convert it to S3A.
      final Path convertedPath = new Path(path);
      final FileSystem fileSystem = convertedPath.getFileSystem(hadoopConfiguration);
      resourceMap = buildResourceMap(convertedPath, fileSystem);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a map of {@link ResourceType} to {@link Dataset} from the given path and file system.
   *
   * @param path the path to the source files
   * @param fileSystem the {@link FileSystem} to use
   * @return a map of {@link ResourceType} to {@link Dataset}
   * @throws IOException if an error occurs while listing the files
   */
  @Nonnull
  private Map<String, Dataset<Row>> buildResourceMap(final @Nonnull Path path,
      @Nonnull final FileSystem fileSystem) throws IOException {
    final FileStatus[] fileStatuses = fileSystem.globStatus(new Path(path, "*"));
    final Map<String, List<String>> fileNamesByResourceType = Stream.of(fileStatuses)
        .map(FileStatus::getPath)
        .map(Object::toString)
        // Filter out any paths that do not have the expected extension.
        .filter(this::checkExtension)
        // Extract the resource code from each path using the file name mapper.
        .flatMap(this::resourceCodeAndPath)
        // Filter out any resource codes that are not supported.
        .filter(p -> context.isResourceTypeSupported(p.getKey()))
        // Group the pairs by resource type, and collect the associated paths into a list.
        .collect(Collectors.groupingBy(Pair::getKey,
            Collectors.mapping(Pair::getValue, Collectors.toList())));

    return fileNamesByResourceType.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            entry -> {
              final String[] paths = entry.getValue().toArray(new String[0]);
              final Dataset<Row> sourceStrings = reader.load(paths);
              return transformer.apply(sourceStrings, entry.getKey());
            }));
  }

  /**
   * Check that the extension of the given path matches the expected extension.
   *
   * @param path the path to check
   * @return true if the extension matches, false otherwise
   */
  private boolean checkExtension(@Nonnull final String path) {
    return FilenameUtils.isExtension(path, extension);
  }

  /**
   * Converts a path to a stream of pairs of resource codes and paths.
   *
   * @param path the path to convert
   * @return a stream of pairs of resource codes and paths
   */
  @Nonnull
  private Stream<Pair<String, String>> resourceCodeAndPath(@Nonnull final String path) {
    final String fileName = FilenameUtils.getBaseName(path);
    return fileNameMapper.apply(fileName).stream()
        .map(resourceType -> Pair.of(resourceType, path));
  }

  private boolean checkResourceSupported(@Nonnull final Pair<String, String> resourceCodeAndPath) {
    final Set<String> unsupported = JavaConverters.setAsJavaSet(
        EncoderBuilder.UNSUPPORTED_RESOURCES());
    final boolean result = unsupported.contains(resourceCodeAndPath.getKey());
    if (result) {
      FileSource.log.warn("Skipping unsupported resource type: {}",
          resourceCodeAndPath.getKey());
    }
    return !result;
  }

}
