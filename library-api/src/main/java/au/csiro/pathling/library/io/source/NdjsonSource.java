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
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A class for making FHIR data within a set of NDJSON files available for query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Slf4j
public class NdjsonSource extends FileSource {

  /**
   * Constructs an NdjsonSource with the specified PathlingContext and path.
   *
   * @param context the PathlingContext to use
   * @param path the path to the NDJSON file or directory
   */
  public NdjsonSource(@Nonnull final PathlingContext context, @Nonnull final String path) {
    // Recognize files with the extension ".ndjson" as NDJSON files, by default.
    this(context, path, "ndjson");
  }

  /**
   * Constructs an NdjsonSource with the specified PathlingContext, path, and extension.
   *
   * @param context the PathlingContext to use
   * @param path the path to the NDJSON file or directory
   * @param extension the file extension to recognize as NDJSON files
   */
  public NdjsonSource(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final String extension) {
    this(context, path, extension,
        // Use the "resource name with qualifier" mapper by default, which takes the resource name
        // from the file name and is tolerant of an optional qualifier string.
        FileSource::resourceNameWithQualifierMapper);
  }

  /**
   * Constructs an NdjsonSource with the specified PathlingContext, path, extension, and file name
   * mapper.
   *
   * @param context the PathlingContext to use
   * @param path the path to the NDJSON file or directory
   * @param extension the file extension to recognize as NDJSON files
   * @param fileNameMapper a function that maps a file name to a set of resource types
   */
  public NdjsonSource(@Nonnull final PathlingContext context, @Nonnull final String path,
      @Nonnull final String extension,
      @Nonnull final Function<String, Set<String>> fileNameMapper) {
    super(context, path, fileNameMapper, extension,
        // Read each line of input separately.
        context.getSpark().read().format("text"),
        // Encode each line of input as a JSON FHIR resource.
        (sourceData, resourceType) -> context.encode(sourceData, resourceType,
            PathlingContext.FHIR_JSON));
  }

  @Nonnull
  @Override
  public NdjsonSource map(@Nonnull final UnaryOperator<Dataset<Row>> operator) {
    return (NdjsonSource) super.map(operator);
  }

  @Override
  public NdjsonSource cache() {
    return map(Dataset::cache);
  }

}
