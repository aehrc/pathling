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

import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for boolean logic.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#boolean-logic">FHIRPath Specification -
 *     Boolean logic</a>
 */
public class BooleanLogicFunctions {

  private BooleanLogicFunctions() {}

  /**
   * Returns {@code true} if the input collection evaluates to {@code false}, and {@code false} if
   * it evaluates to {@code true}.
   *
   * <p>Otherwise, the result is empty.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the negated values
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#not--boolean">not</a>
   */
  @Nonnull
  @SqlOnFhirConformance(Profile.SHARABLE)
  @FhirPathFunction
  public static BooleanCollection not(@Nonnull final Collection input) {
    return BooleanCollection.build(input.asBooleanSingleton().getColumn().not());
  }
}
