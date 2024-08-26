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

package au.csiro.pathling.io.source;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import scala.compat.java8.OptionConverters;

/**
 * Common functionality for file-based sources.
 *
 * @author John Grimes
 */
@Slf4j
public abstract class FileSource extends DatasetSource {

  @NotNull
  private final Function<String, Set<String>> fileNameMapper;

  @NotNull
  protected final String extension;

  @NotNull
  protected final BiFunction<String, List<String>, Dataset<Row>> reader;

  protected FileSource(@NotNull final String path,
      @NotNull final Function<String, Set<String>> fileNameMapper, @NotNull final String extension,
      @NotNull final BiFunction<String, List<String>, Dataset<Row>> reader) {
    super();
    this.fileNameMapper = fileNameMapper;
    this.extension = extension;
    this.reader = reader;

    final Optional<SparkSession> spark = OptionConverters.toJava(SparkSession.getActiveSession());
    final org.apache.hadoop.conf.Configuration hadoopConfiguration = spark
        .orElseThrow(() -> new IllegalStateException("No active Spark session"))
        .sparkContext()
        .hadoopConfiguration();
    try {
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
  @NotNull
  private Map<ResourceType, Dataset<Row>> buildResourceMap(@NotNull final Path path,
      @NotNull final FileSystem fileSystem) throws IOException {
    final FileStatus[] fileStatuses = fileSystem.globStatus(new Path(path, "*"));
    final Map<ResourceType, List<String>> fileNamesByResourceType = Stream.of(fileStatuses)
        .map(FileStatus::getPath)
        .map(Object::toString)
        // Filter out any paths that do not have the expected extension.
        .filter(this::checkExtension)
        // Extract the resource code from each path using the file name mapper.
        .flatMap(this::resourceCodeAndPath)
        // Parse the resource code.
        .map(this::resourceTypeAndPath)
        // Filter out any resource types that were not valid.
        .filter(Objects::nonNull)
        // Group the pairs by resource type, and collect the associated paths into a list.
        .collect(
            groupingBy(ResourceTypeAndPath::type, mapping(ResourceTypeAndPath::path, toList())));

    return fileNamesByResourceType.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            entry -> reader.apply(entry.getKey().toCode(), entry.getValue())));
  }

  /**
   * Check that the extension of the given path matches the expected extension.
   *
   * @param path the path to check
   * @return true if the extension matches, false otherwise
   */
  private boolean checkExtension(@NotNull final String path) {
    return FilenameUtils.isExtension(path, extension);
  }

  /**
   * Converts a path to a stream of pairs of resource codes and paths.
   *
   * @param path the path to convert
   * @return a stream of pairs of resource codes and paths
   */
  @NotNull
  private Stream<ResourceCodeAndPath> resourceCodeAndPath(@NotNull final String path) {
    final String fileName = FilenameUtils.getBaseName(path);
    final Set<String> paths = requireNonNull(fileNameMapper.apply(fileName),
        "Paths must not be null");
    return paths.stream()
        .map(resourceType -> new ResourceCodeAndPath(resourceType, path));
  }

  /**
   * Converts a pair of resource code and path to a pair of {@link ResourceType} and path.
   *
   * @param resourceCodeAndPath the pair of resource code and path to convert
   * @return a pair of {@link ResourceType} and path, or null if the resource code was not valid
   */
  @Nullable
  private ResourceTypeAndPath resourceTypeAndPath(
      @NotNull final ResourceCodeAndPath resourceCodeAndPath) {
    @Nullable final ResourceType resourceType;
    try {
      resourceType = ResourceType.fromCode(resourceCodeAndPath.code());
    } catch (final FHIRException e) {
      return null;
    }
    return new ResourceTypeAndPath(resourceType, resourceCodeAndPath.path());
  }

}
