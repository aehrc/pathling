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

package au.csiro.pathling.fhirpath.function.provider;

import static au.csiro.pathling.fhirpath.comparison.Equatable.EqualityOperation.EQUALS;

import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Additional FHIRPath functions that are defined within the FHIR specification.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">FHIR specification - Additional
 *     functions</a>
 */
public class FhirFunctions {

  private static final String URL_ELEMENT_NAME = "url";
  private static final String EXTENSION_ELEMENT_NAME = "extension";

  private FhirFunctions() {}

  /**
   * Will filter the input collection for items named "extension" with the given url. This is a
   * syntactical shortcut for {@code .extension.where(url = string)}, but is simpler to write. Will
   * return an empty collection if the input collection is empty or the url is empty.
   *
   * @param input The input collection
   * @param url The URL to filter by
   * @return A collection containing only those elements for which the criteria expression evaluates
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection extension(
      @Nonnull final Collection input, @Nonnull final StringCollection url) {
    // Traverse to the "extension" child element.
    return input
        .traverse(EXTENSION_ELEMENT_NAME)
        .map(
            extensionCollection ->
                // Filter the "extension" collection for items with the specified URL.
                FilteringAndProjectionFunctions.where(
                    extensionCollection,
                    // Traverse to the "url" child element of the extension.
                    c ->
                        c.traverse(URL_ELEMENT_NAME)
                            .filter(StringCollection.class::isInstance)
                            // Use the comparison operation to check if the URL matches the input
                            // URL.
                            .map(
                                urlCollection ->
                                    urlCollection.getElementEquality(EQUALS).apply(url))
                            // If the URL is present, build a BooleanCollection from the result.
                            .map(BooleanCollection::build)
                            // If the URL is not present, return a false Boolean literal.
                            .orElse(BooleanCollection.fromValue(false))))
        .orElse(EmptyCollection.getInstance());
  }
}
