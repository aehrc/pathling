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

import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Set;
import java.util.function.Function;

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
  ParquetSource(@Nonnull final PathlingContext context, @Nonnull final String path) {
    super(context, path,
        // Use the "resource name with qualifier" mapper by default, which takes the resource name
        // from the file name and is tolerant of an optional qualifier string.
        FileSource::resourceNameWithQualifierMapper,
        // Assume the "parquet" file extension.
        PARQUET_FILE_EXTENSION,
        context.getSpark().read().format(PARQUET_READ_FORMAT),
        // Apply no transformations on the data - we assume it has already been processed using the 
        // Pathling FHIR encoders.
        (sourceData, resourceType) -> sourceData);
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
        (sourceData, resourceType) -> sourceData);
  }

}
