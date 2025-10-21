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

import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A class for making FHIR data in Parquet format available for query. It is assumed that the schema
 * of the Parquet files aligns with that of the Pathling FHIR encoders.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class ParquetSource extends FileSource {

  private static final String PARQUET_FILE_EXTENSION = "parquet";
  private static final String PARQUET_READ_FORMAT = "parquet";

  /**
   * Constructs a ParquetSource with the specified PathlingContext and path.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Parquet file or directory
   */
  ParquetSource(@Nonnull final PathlingContext context, @Nonnull final String path, @Nonnull final Predicate<ResourceType> additionalResourceTypeFilter) {
    super(context, path, null,
        // Assume the "parquet" file extension.
        PARQUET_FILE_EXTENSION,
        context.getSpark().read().format(PARQUET_READ_FORMAT),
        // Apply no transformations on the data - we assume it has already been processed using the 
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        additionalResourceTypeFilter);
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext, path, and custom file name
   * mapper.
   *
   * @param context the PathlingContext to use
   * @param path the path to the Parquet file or directory
   * @param fileNameMapper a function that maps a file name to a set of resource types
   */
  ParquetSource(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final Function<String, Set<String>> fileNameMapper) {
    super(context, path, fileNameMapper, PARQUET_FILE_EXTENSION,
        context.getSpark().read().format(PARQUET_READ_FORMAT),
        // Apply no transformations on the data - we assume it has already been processed using the 
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true);
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext and map of resource types to files.
   *
   * @param context the PathlingContext to use
   * @param files a map where keys are resource type names and values are collections of file paths
   */
  ParquetSource(@Nonnull final PathlingContext context, @Nonnull final Map<String, Collection<String>> files,
      @Nonnull final Predicate<ResourceType> additionalResourceTypeFilter) {
    super(context, files, null,
        // Assume the "parquet" file extension.
        PARQUET_FILE_EXTENSION,
        context.getSpark().read().format(PARQUET_READ_FORMAT),
        // Apply no transformations on the data - we assume it has already been processed using the 
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        additionalResourceTypeFilter);
  }

  /**
   * Constructs a ParquetSource with the specified PathlingContext, map of files, and custom
   * file name mapper.
   *
   * @param context the PathlingContext to use
   * @param files a map where keys are resource type names and values are collections of file paths
   * @param fileNameMapper a function that maps a file name to a set of resource types
   */
  ParquetSource(@Nonnull final PathlingContext context, @Nonnull final Map<String, Collection<String>> files,
      @Nonnull final Function<String, Set<String>> fileNameMapper) {
    super(context, files, fileNameMapper, PARQUET_FILE_EXTENSION,
        context.getSpark().read().format(PARQUET_READ_FORMAT),
        // Apply no transformations on the data - we assume it has already been processed using the 
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData,
        resourceType -> true);
  }

}
