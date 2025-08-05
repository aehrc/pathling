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
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * A class for making FHIR data within a set of NDJSON files available for query.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Slf4j
public class NdjsonSource extends FileSource {

  // Matches a base name that consists of a resource type, optionally followed by a period and a
  // qualifier string. The first group will contain the resource type, and the second group will
  // contain the qualifier string (if present).
  private static final Pattern BASE_NAME_WITH_QUALIFIER = Pattern.compile(
      "^([A-Za-z]+)(\\.[^.]+)?$");

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
        NdjsonSource::resourceNameWithQualifierMapper);
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
  public static Set<String> resourceNameWithQualifierMapper(@Nonnull final String baseName) {
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
