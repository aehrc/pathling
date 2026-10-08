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

package au.csiro.pathling.library.io.source;

import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.types.StructType;

/**
 * A class for making FHIR data in Parquet format available for query. It is assumed that the schema
 * of the Parquet files aligns with that of the Pathling FHIR encoders.
 *
 * <p>The files of one resource type may carry differing schemas, because a schema fitted to the
 * data widens as new elements first appear in it. They are read as one dataset whose schema is the
 * union of theirs (FR-041): without merging, the schema of whichever file is listed first wins and
 * the columns only the others carry are silently dropped. Merging reads the footer of every file of
 * the type, so the cost of it can be avoided per resource type by supplying the schema to read
 * with. A supplied schema is used as given, so it must cover the union of the type's files, or the
 * columns it omits are dropped just as silently. It can lose whole rows, not just columns: a view
 * unnesting a repeating element returns none of a file's rows for that element when the schema
 * omits every field the file carries inside it. The workable pattern is to merge once and persist
 * the result, or to take the writer's schema, and supply that. There is deliberately no switch that
 * turns merging off without supplying a schema (T119).
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@SuppressWarnings("unused")
public class ParquetSource extends FileSource {

  private static final String PARQUET_FILE_EXTENSION = "parquet";
  private static final String PARQUET_READ_FORMAT = "parquet";

  /** The schema to read each resource type with, in place of merging the schemas of its files. */
  @Nonnull private final Map<String, StructType> schemas;

  /**
   * Constructs a ParquetSource with the specified PathlingContext and path.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Parquet file or directory
   */
  public ParquetSource(@Nonnull final PathlingContext context, @Nonnull final String path) {
    this(context, path, (Predicate<String>) ignored -> true);
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext, path, and additional resource
   * type filter.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Parquet file or directory
   * @param additionalResourceTypeFilter predicate to filter resource types to be loaded, receives
   *     the resource type code as a string
   */
  public ParquetSource(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final Predicate<String> additionalResourceTypeFilter) {
    super(
        context,
        path,
        // Use the "resource name with qualifier" mapper by default, which takes the resource name
        // from the file name and is tolerant of an optional qualifier string.
        FileSource::resourceNameWithQualifierMapper,
        // Assume the "parquet" file extension.
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        additionalResourceTypeFilter);
    this.schemas = Map.of();
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext, path, and custom file name
   * mapper.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Parquet file or directory
   * @param fileNameMapper a function that maps a file name to a set of resource types
   */
  public ParquetSource(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final Function<String, Set<String>> fileNameMapper) {
    super(
        context,
        path,
        fileNameMapper,
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true);
    this.schemas = Map.of();
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext and map of resource types to
   * files.
   *
   * @param context the PathlingContext to use
   * @param files a map where keys are resource type names and values are collections of file paths
   * @param additionalResourceTypeFilter predicate to filter resource types to be loaded, receives
   *     the resource type code as a string
   */
  public ParquetSource(
      @Nonnull final PathlingContext context,
      @Nonnull final Map<String, Collection<String>> files,
      @Nonnull final Predicate<String> additionalResourceTypeFilter) {
    super(
        context,
        files,
        // Assume the "parquet" file extension.
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        additionalResourceTypeFilter);
    this.schemas = Map.of();
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext, map of files, and custom file
   * name mapper.
   *
   * @param context the PathlingContext to use
   * @param files a map where keys are resource type names and values are collections of file paths
   * @param fileNameMapper a function that maps a file name to a set of resource types
   */
  public ParquetSource(
      @Nonnull final PathlingContext context,
      @Nonnull final Map<String, Collection<String>> files,
      @Nonnull final Function<String, Set<String>> fileNameMapper) {
    super(
        context,
        files,
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true);
    this.schemas = Map.of();
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext and path, reading each resource
   * type named in the given map with the schema it supplies rather than merging the schemas of its
   * files. Resource types the map does not name are merged as usual.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Parquet file or directory
   * @param schemas the schema to read each resource type with, keyed by resource type code
   */
  public ParquetSource(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final Map<String, StructType> schemas) {
    super(
        context,
        path,
        FileSource::resourceNameWithQualifierMapper,
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true);
    this.schemas = Map.copyOf(schemas);
  }

  /**
   * Returns a reader for a resource type: one bound to the supplied schema where there is one, and
   * otherwise the shared reader, which merges. A fresh reader is built for a supplied schema
   * because a reader is mutable, and binding a schema to the shared one would bind it for every
   * type read after it.
   */
  @Nonnull
  @Override
  protected DataFrameReader readerFor(@Nonnull final String resourceType) {
    return Optional.ofNullable(schemas.get(resourceType))
        .map(schema -> context.getSpark().read().format(PARQUET_READ_FORMAT).schema(schema))
        .orElse(reader);
  }

  /**
   * Returns a reader that merges the schemas of the files it reads into their union.
   *
   * @param context the PathlingContext to use
   * @return the reader
   */
  @Nonnull
  private static DataFrameReader mergingReader(@Nonnull final PathlingContext context) {
    return context.getSpark().read().format(PARQUET_READ_FORMAT).option("mergeSchema", "true");
  }
}
