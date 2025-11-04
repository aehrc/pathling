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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
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
   * An additional filter to exclude specific resource types from being processed. This filter is
   * applied after standard resource type validation and extension checking. Use this to selectively
   * exclude resource types that should not be loaded, even if they are present in the source files
   * and would otherwise be valid.
   */
  @Nonnull
  protected final Predicate<ResourceType> additionalResourceTypeFilter;

  /**
   * Path-based constructor that scans a directory to discover FHIR data files.
   * <p>
   * Use this constructor when you have a directory containing FHIR files and want Pathling to
   * automatically discover and group them by resource type based on file names. This is the typical
   * approach for reading from a file system directory where files follow a naming convention (e.g.,
   * {@code Patient.ndjson}, {@code Observation.parquet}).
   * <p>
   * The constructor will scan the specified path and use either the provided {@code fileNameMapper}
   * or the default naming convention ({@code <resource_type>.<extension>}) to determine which files
   * contain which resource types. Files may also include a partition identifier (e.g.,
   * {@code Patient.00000.ndjson}) which will be handled automatically.
   *
   * @param context the Pathling context
   * @param path the path to the source files, which may be a directory or a glob pattern
   * @param fileNameMapper a function that maps a file name to a set of resource types, or null to
   * use the default naming convention
   * @param extension the file extension that this source expects for its source files
   * @param reader a {@link DataFrameReader} that can be used to read the source files
   * @param transformer a function that transforms a {@link Dataset<Row>} containing raw source data
   * of a specified resource type into a {@link Dataset<Row>} containing the imported data
   * @param additionalResourceTypeFilter filter to filter out specific resource types if desired
   */
  protected FileSource(@Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final Function<String, Set<String>> fileNameMapper, @Nonnull final String extension,
      @Nonnull final DataFrameReader reader,
      @Nonnull final BiFunction<Dataset<Row>, String, Dataset<Row>> transformer,
      @Nonnull final Predicate<ResourceType> additionalResourceTypeFilter) {
    this(context, retrieveFilesFromPath(path, context, fileNameMapper), extension, reader,
        transformer, additionalResourceTypeFilter);
  }


  /**
   * Map-based constructor for programmatically specifying which files contain which resource
   * types.
   * <p>
   * Use this constructor when you already know the mapping between resource types and file paths,
   * and want to provide this information explicitly rather than relying on file name conventions.
   * This is particularly useful for:
   * <ul>
   *   <li>FHIR Bulk Data operations where file lists are provided via manifests</li>
   *   <li>Custom file organisation schemes that don't follow standard naming conventions</li>
   *   <li>Programmatic data source construction where file-to-resource mappings are computed</li>
   *   <li>Reading from multiple partitioned files (e.g., {@code Patient.00000.ndjson},
   *       {@code Patient.00001.ndjson}) for a single resource type</li>
   * </ul>
   * <p>
   * Unlike the path-based constructor, this does not scan directories or infer resource types from
   * file names. All file-to-resource-type mappings must be provided explicitly in the
   * {@code files} parameter.
   *
   * @param context the Pathling context
   * @param files a map where keys are resource type codes (e.g., "Patient", "Observation") and
   * values are collections of file paths containing data for that resource type
   * @param extension the file extension that this source expects for its source files
   * @param reader a {@link DataFrameReader} that can be used to read the source files
   * @param transformer a function that transforms a {@link Dataset<Row>} containing raw source data
   * of a specified resource type into a {@link Dataset<Row>} containing the imported data
   * @param additionalResourceTypeFilter filter to filter out specific resource types if desired
   */
  protected FileSource(@Nonnull final PathlingContext context,
      @Nonnull final Map<String, Collection<String>> files, @Nonnull final String extension,
      @Nonnull final DataFrameReader reader,
      @Nonnull final BiFunction<Dataset<Row>, String, Dataset<Row>> transformer,
      @Nonnull final Predicate<ResourceType> additionalResourceTypeFilter) {
    super(context);
    this.extension = extension;
    this.reader = reader;
    this.transformer = transformer;
    this.additionalResourceTypeFilter = additionalResourceTypeFilter;
    this.resourceMap = buildResourceMap(files);
  }

  @Nonnull
  private static Map<String, Collection<String>> retrieveFilesFromPath(@Nonnull final String path,
      @Nonnull final PathlingContext context,
      @Nonnull final Function<String, Set<String>> fileNameMapper) {
    final org.apache.hadoop.conf.Configuration hadoopConfiguration = requireNonNull(
        context.getSpark().sparkContext().hadoopConfiguration());
    try {
      final Path convertedPath = new Path(path);
      final FileSystem fileSystem = convertedPath.getFileSystem(hadoopConfiguration);
      final FileStatus[] fileStatuses = fileSystem.globStatus(new Path(path, "*"));
      // First group by resource type.
      final Map<String, Collection<String>> groupedByDefaultNamingAssumption = Arrays.stream(
              fileStatuses)
          .map(FileStatus::getPath)
          .map(Path::toString)
          .collect(Collectors.groupingBy(FileSource::retrieveResourceTypeFromFilePath,
              Collectors.toCollection(HashSet::new)));
      // Apply the file name mapper.
      return groupedByDefaultNamingAssumption.entrySet().stream()
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              Map.Entry::getValue
          ));

    } catch (final IOException e) {
      throw new PersistenceError("Problem reading source files from file system", e);
    }
  }

  private static String retrieveResourceTypeFromFilePath(String filePath) {
    String fileName = FilenameUtils.getBaseName(filePath);
    final String[] split = fileName.split("\\.");
    if (split.length == 2) {
      // has partition id like '<resource_type>.<partition_id>'
      fileName = split[0];
    }
    return fileName;
  }

  /**
   * Creates a map of {@link ResourceType} to {@link Dataset} from the given map of resource types
   * to files.
   *
   * @param files a map of resource types to file paths
   * @return a map of {@link ResourceType} to {@link Dataset}
   */
  @Nonnull
  private Map<String, Dataset<Row>> buildResourceMap(
      final @Nonnull Map<String, Collection<String>> files) {
    return files.entrySet().stream()
        // Filter out any paths that do not have the expected extension.
        .map(entry -> Map.entry(entry.getKey(), checkExtension(entry.getValue())))
        // Filter out resource types with no files after extension filtering.
        .filter(entry -> !entry.getValue().isEmpty())
        // Filter out any resource codes that are not supported.
        .filter(entry -> context.isResourceTypeSupported(entry.getKey()))
        // Filter out any resource that should be explicitly ignored
        .filter(entry -> additionalResourceTypeFilter.test(ResourceType.fromCode(entry.getKey())))
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
   * @param paths the paths to check
   * @return a filtered collection with elements where the extension matches
   */
  private Collection<String> checkExtension(@Nonnull final Collection<String> paths) {
    return paths.stream()
        .filter(path -> FilenameUtils.isExtension(path, extension))
        .collect(Collectors.toSet());
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

}
