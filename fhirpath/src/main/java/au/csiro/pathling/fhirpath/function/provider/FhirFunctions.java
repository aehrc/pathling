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

import static au.csiro.pathling.fhirpath.comparison.Equatable.EqualityOperation.EQUALS;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Additional FHIRPath functions that are defined within the FHIR specification.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#functions">FHIR specification - Additional
 * functions</a>
 */
public class FhirFunctions {

  private static final String URL_ELEMENT_NAME = "url";
  private static final String EXTENSION_ELEMENT_NAME = "extension";

  private FhirFunctions() {
  }

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
  public static Collection extension(@Nonnull final Collection input,
      @Nonnull final StringCollection url) {
    // Traverse to the "extension" child element.
    return input.traverse(EXTENSION_ELEMENT_NAME).map(extensionCollection ->
        // Filter the "extension" collection for items with the specified URL.
        FilteringAndProjectionFunctions.where(extensionCollection,
            // Traverse to the "url" child element of the extension.
            c -> c.traverse(URL_ELEMENT_NAME)
                .filter(StringCollection.class::isInstance)
                // Use the comparison operation to check if the URL matches the input URL.
                .map(urlCollection ->
                    urlCollection.getElementEquality(EQUALS).apply(url))
                // If the URL is present, build a BooleanCollection from the result.
                .map(BooleanCollection::build)
                // If the URL is not present, return a false Boolean literal.
                .orElse(BooleanCollection.fromValue(false)))
    ).orElse(EmptyCollection.getInstance());
  }

  /**
   * Returns a collection containing type information for resolved references.
   * <p>
   * This is a limited implementation of the {@code resolve()} function that supports type checking
   * with the {@code is} operator, but does not perform actual resource resolution. The type
   * information is extracted from:
   * <ol>
   *   <li>The {@code Reference.type} field (if present) - takes precedence</li>
   *   <li>The resource type parsed from the {@code Reference.reference} string (if type field is absent)</li>
   * </ol>
   * <p>
   * Type extraction behavior:
   * <ul>
   *   <li>If {@code Reference.type} is present, it is used regardless of reference format (including
   *       contained and logical references)</li>
   *   <li>If {@code Reference.type} is absent, type is parsed from {@code Reference.reference} for
   *       relative, absolute, and canonical reference formats</li>
   *   <li>Returns empty if type cannot be determined from either field</li>
   * </ul>
   * <p>
   * Supported reference formats (when type field is absent):
   * <ul>
   *   <li>Relative: {@code Patient/123} → Patient</li>
   *   <li>Absolute: {@code http://example.org/fhir/Patient/123} → Patient</li>
   *   <li>Canonical: {@code http://hl7.org/fhir/ValueSet/my-valueset} → ValueSet</li>
   * </ul>
   * <p>
   * Returns empty when type cannot be determined:
   * <ul>
   *   <li>Contained references without type field: {@code #local-id}</li>
   *   <li>Logical references without type field (identifier-only)</li>
   *   <li>Malformed or unparseable reference strings</li>
   * </ul>
   * <p>
   * <b>Important:</b> The returned collection does not support traversal to child elements.
   * Attempting to access fields like {@code resolve().name} will throw an
   * {@link au.csiro.pathling.errors.UnsupportedFhirPathFeatureError}.
   * <p>
   * Examples:
   * <pre>
   *   // Supported - type checking with is operator
   *   Appointment.participant.actor.where(resolve() is Location)
   *   actor.resolve() is Patient
   *
   *   // Contained reference with type field
   *   Reference { type: "Patient", reference: "#contained-1" } → resolves to Patient
   *
   *   // Type field takes precedence
   *   Reference { type: "Organization", reference: "Patient/123" } → resolves to Organization
   *
   *   // NOT supported - throws UnsupportedFhirPathFeatureError
   *   actor.resolve().name
   * </pre>
   *
   * @param input The input collection (must be a ReferenceCollection)
   * @return A collection containing type information for resolvable references
   * @throws InvalidUserInputError if the input is not a ReferenceCollection
   * @see <a href="https://build.fhir.org/fhirpath.html#functions">FHIRPath resolve() function</a>
   * @see <a href="https://hl7.org/fhir/R4/references.html">FHIR Resource References</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.EXPERIMENTAL)
  @Nonnull
  public static Collection resolve(@Nonnull final Collection input) {
    // Validate that input is a ReferenceCollection and delegate to its resolve() method
    if (!(input instanceof final ReferenceCollection referenceCollection)) {
      throw new InvalidUserInputError(
          "resolve() can only be called on Reference elements, got: " + input.getClass()
              .getSimpleName());
    }

    return referenceCollection.resolve();
  }
}
