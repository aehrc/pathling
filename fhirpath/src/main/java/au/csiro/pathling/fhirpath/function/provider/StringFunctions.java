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

package au.csiro.pathling.fhirpath.function.provider;

import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Contains functions for manipulating strings.
 *
 * @see <a
 * href="https://build.fhir.org/ig/HL7/FHIRPath/#wherecriteria--expression--collection">FHIRPath
 * Specification - Additional String Functions</a>
 */
public class StringFunctions {

  private static final String JOIN_DEFAULT_SEPARATOR = "";

  private StringFunctions() {
  }

  /**
   * The join function takes a collection of strings and joins them into a single string, optionally
   * using the given separator.
   * <p>
   * If the input is empty, the result is empty.
   * <p>
   * If no separator is specified, the strings are directly concatenated.
   *
   * @param input The input in
   * @param separator The separator to use
   * @return A {@link StringCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#joinseparator-string--string">FHIRPath
   * Specification - join</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.EXPERIMENTAL)
  @Nonnull
  public static StringCollection join(@Nonnull final StringCollection input,
      @Nullable final StringCollection separator) {
    return StringCollection.build(input.getColumn().join(
        nonNull(separator)
        ? separator.asSingular().getColumn()
        : DefaultRepresentation.literal(JOIN_DEFAULT_SEPARATOR)
    ));
  }

}
