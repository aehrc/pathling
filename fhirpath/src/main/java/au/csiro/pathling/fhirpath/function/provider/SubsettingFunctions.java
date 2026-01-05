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
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for subsetting collections.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#subsetting">FHIRPath Specification -
 *     Subsetting</a>
 */
public class SubsettingFunctions {

  private SubsettingFunctions() {}

  /**
   * Returns a collection containing only the first item in the input collection. This function is
   * equivalent to {@code item[0]}, so it will return an empty collection if the input collection
   * has no items.
   *
   * @param input The input collection
   * @return A collection containing only the first item in the input collection
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#first--collection">FHIRPath Specification
   *     - first</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection first(@Nonnull final Collection input) {
    return input.copyWith(input.getColumn().first());
  }
}
