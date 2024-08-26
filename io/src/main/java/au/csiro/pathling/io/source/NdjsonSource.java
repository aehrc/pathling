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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import au.csiro.pathling.schema.FhirJsonReader;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

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

  public NdjsonSource(@NotNull final String path, @NotNull final String fhirVersion) {
    // Recognize files with the extension ".ndjson" as NDJSON files, by default.
    this(path, "ndjson", fhirVersion);
  }

  private NdjsonSource(@NotNull final String path, @NotNull final String extension,
      @NotNull final String fhirVersion) {
    this(path, extension,
        // Use the "resource name with qualifier" mapper by default, which takes the resource name
        // from the file name and is tolerant of an optional qualifier string.
        NdjsonSource::resourceNameWithQualifierMapper,
        fhirVersion);
  }

  private NdjsonSource(@NotNull final String path, @NotNull final String extension,
      @NotNull final Function<String, Set<String>> fileNameMapper,
      @NotNull final String fhirVersion) {
    super(path, fileNameMapper, extension, (resourceType, paths) ->
        new FhirJsonReader(resourceType, fhirVersion, emptyMap()).read(paths.toArray(new String[0]))
    );
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
  @NotNull
  private static Set<String> resourceNameWithQualifierMapper(@NotNull final String baseName) {
    final Matcher matcher = BASE_NAME_WITH_QUALIFIER.matcher(baseName);
    // If the base name does not match the expected format, return an empty set.
    if (!matcher.matches()) {
      return emptySet();
    }
    // If the base name does not contain a qualifier, return the base name as-is.
    if (matcher.group(2) == null) {
      return singleton(baseName);
    }
    // If the base name contains a qualifier, remove it and return the base name without the
    // qualifier.
    final String qualifierRemoved = new StringBuilder(baseName).replace(matcher.start(2),
        matcher.end(2), "").toString();
    return singleton(qualifierRemoved);
  }

}
