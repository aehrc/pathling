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

import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for type checking and type operations.
 *
 * @author Piotr Szul
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#types">FHIRPath Specification - Types</a>
 */
public class TypeFunctions {

  private TypeFunctions() {
  }

  /**
   * Returns true collection if the input collection contains a single item of the given type or a subclass
   * thereof. Returns false collection if the input contains a single item that is not of the specified type.
   * Returns empty if the input collection is empty. Throws an error if the input collection
   * contains more than one item.
   * <p>
   * The type argument is an identifier that must resolve to the name of a type in a model. For
   * implementations with compile-time typing, this requires special-case handling when processing
   * the argument to treat it as a type specifier rather than an identifier expression.
   *
   * @param input The input collection
   * @param typeSpecifier The type specifier
   * @return A boolean collection containing the result of type matching, or empty
   * @see <a href="https://hl7.org/fhirpath/#istype--type-specifier">is</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection is(@Nonnull final Collection input,
      @Nonnull final TypeSpecifier typeSpecifier) {
    return input.isOfType(typeSpecifier);
  }

  /**
   * Returns the value of the input collection if it contains a single item of the given type or a
   * subclass thereof. Returns empty collection if the input contains a single item that is not of
   * the specified type. Returns empty if the input collection is empty. Throws an error if the
   * input collection contains more than one item.
   * <p>
   * The type argument is an identifier that must resolve to the name of a type in a model. For
   * implementations with compile-time typing, this requires special-case handling when processing
   * the argument to treat it as a type specifier rather than an identifier expression.
   *
   * @param input The input collection
   * @param typeSpecifier The type specifier
   * @return The input value if type matches, or empty collection otherwise
   * @see <a href="https://hl7.org/fhirpath/#astype--type-specifier">as</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection as(@Nonnull final Collection input,
      @Nonnull final TypeSpecifier typeSpecifier) {
    return input.asType(typeSpecifier);
  }

}
