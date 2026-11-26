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

import au.csiro.pathling.io.SchemaSet;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.spark.sql.DataFrameReader;

/**
 * A class for making FHIR data in Parquet format available for query. It is assumed that the schema
 * of the Parquet files aligns with that of the Pathling FHIR encoders.
 *
 * <p>The files of one resource type may carry differing schemas, because a schema fitted to the
 * data widens as new elements first appear in it. They are read as one dataset whose schema is the
 * union of theirs (FR-041): without merging, the schema of whichever file is listed first wins and
 * the columns only the others carry are silently dropped. A resource type a schema set names is
 * merged in the same way, and the merged dataset is then projected onto the set's schema, so the
 * set chooses which columns the read returns but does not avoid reading the footer of every file.
 *
 * <p>The layout of each resource type's data is classified from its schema as it is first read, and
 * data in a layout this release does not read, such as the layout written by earlier releases, is
 * refused with an {@link UnsupportedLayoutException} (FR-037). This can be disabled per source.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@SuppressWarnings("unused")
public class ParquetSource extends FileSource {

  private static final String PARQUET_FILE_EXTENSION = "parquet";
  private static final String PARQUET_READ_FORMAT = "parquet";

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
        // Use the "resource name with qualifier" mapper by default, which takes the resource name
        // from the file name and is tolerant of an optional qualifier string.
        retrieveFilesFromPath(path, context, FileSource::resourceNameWithQualifierMapper),
        // Assume the "parquet" file extension.
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        additionalResourceTypeFilter,
        true);
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
        retrieveFilesFromPath(path, context, fileNameMapper),
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true,
        true);
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
        additionalResourceTypeFilter,
        true);
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
        resourceType -> true,
        true);
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext and path, with full control over
   * how files are mapped to resource types, which resource types are projected onto a schema and
   * whether the layout of the stored data is classified.
   *
   * <p>Layout detection is enabled by every other constructor. It classifies each resource type's
   * data from the schema the read already resolves, and refuses data in a layout this release does
   * not read, such as the layout written by earlier releases, with an {@link
   * UnsupportedLayoutException}. Disabling it is an opt-out for conforming data the detector
   * misclassifies, and applies to this source alone (FR-040). Data in the earlier layout read with
   * it disabled is not supported, and the results of a query over it are undefined.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Parquet file or directory
   * @param fileNameMapper a function that maps a file name to a set of resource types
   * @param schemaSet the schema set, whose types are projected onto their schemas as they are read;
   *     types it does not name are read as stored
   * @param layoutDetection whether the layout of each resource type's data is classified as it is
   *     read
   */
  ParquetSource(
      @Nonnull final PathlingContext context,
      @Nonnull final String path,
      @Nonnull final Function<String, Set<String>> fileNameMapper,
      @Nonnull final SchemaSet schemaSet,
      final boolean layoutDetection) {
    super(
        context,
        retrieveFilesFromPath(path, context, fileNameMapper),
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // The data is assumed to be in this layout already, so only a type the schema set names is
        // transformed, by projecting it onto the set's schema.
        DefinedSchemaReads.of(context, schemaSet)::project,
        resourceType -> true,
        layoutDetection);
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext and map of resource types to
   * files, saying whether the layout of the stored data is classified as it is read. See {@link
   * #ParquetSource(PathlingContext, String, Function, SchemaSet, boolean)} for what layout
   * detection does.
   *
   * @param context the PathlingContext to use
   * @param files a map where keys are resource type names and values are collections of file paths
   * @param additionalResourceTypeFilter predicate to filter resource types to be loaded, receives
   *     the resource type code as a string
   * @param layoutDetection whether the layout of each resource type's data is classified as it is
   *     read
   */
  public ParquetSource(
      @Nonnull final PathlingContext context,
      @Nonnull final Map<String, Collection<String>> files,
      @Nonnull final Predicate<String> additionalResourceTypeFilter,
      final boolean layoutDetection) {
    super(
        context,
        files,
        PARQUET_FILE_EXTENSION,
        mergingReader(context),
        // Apply no transformations on the data - we assume it has already been processed using the
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        additionalResourceTypeFilter,
        layoutDetection);
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
