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

package au.csiro.pathling.library.io.source;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.PersistenceError;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

/**
 * Common functionality for file-based sources.
 *
 * @author John Grimes
 */
@Slf4j
public abstract class FileSource extends DatasetSource {

  // Matches a base name that consists of a resource type, optionally followed by a period and a
  // qualifier string. The first group will contain the resource type, and the second group will
  // contain the qualifier string (if present).
  static final Pattern BASE_NAME_WITH_QUALIFIER = Pattern.compile(
      "^([A-Za-z]+)(\\.[^.]+)?$");
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
  
  @Nonnull
  protected final Predicate<ResourceType> additionalResourceTypeFilter;

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
      @Nonnull final BiFunction<Dataset<Row>, String, Dataset<Row>> transformer,
      @Nonnull Predicate<ResourceType> additionalResourceTypeFilter) {
    super(context);
    this.fileNameMapper = fileNameMapper;
    this.extension = extension;
    this.reader = reader;
    this.transformer = transformer;
    this.additionalResourceTypeFilter = additionalResourceTypeFilter;

    final org.apache.hadoop.conf.Configuration hadoopConfiguration = requireNonNull(
        context.getSpark().sparkContext().hadoopConfiguration());
    try {
      final Path convertedPath = new Path(path);
      final FileSystem fileSystem = convertedPath.getFileSystem(hadoopConfiguration);
      resourceMap = buildResourceMap(convertedPath, fileSystem);
    } catch (final IOException e) {
      throw new PersistenceError("Problem reading source files from file system", e);
    }
  }

  /**
   * Extracts the resource type from the provided base name. Allows for an optional qualifier
   * string, which is separated from the resource name by a period. For example, "Procedure.ICU"
   * will return ["Procedure"].
   * <p>
   * This method does not validate that the resource type is a valid FHIR resource type.
   *
   * @param baseName the base name of the file
   * @return a single-element set containing the resource type, or an empty set if the base name
   * does not match the expected format
   */
  @Nonnull
  static Set<String> resourceNameWithQualifierMapper(@Nonnull final String baseName) {
    final Matcher matcher = BASE_NAME_WITH_QUALIFIER.matcher(baseName);
    // If the base name does not match the expected format, return an empty set.
    if (!matcher.matches()) {
      return Collections.emptySet();
    }
    // If the base name does not contain a qualifier, return the base name as-is.
    if (matcher.group(2) == null) {
      return Collections.singleton(baseName);
    }
    // If the base name contains a qualifier, remove it and return the base name without the
    // qualifier.
    final String qualifierRemoved = new StringBuilder(baseName).replace(matcher.start(2),
        matcher.end(2), "").toString();
    return Collections.singleton(qualifierRemoved);
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
        // Filter out any resource that should be explicitly ignored
        .filter(p -> additionalResourceTypeFilter.test(ResourceType.fromCode(p.getKey())))
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

}
