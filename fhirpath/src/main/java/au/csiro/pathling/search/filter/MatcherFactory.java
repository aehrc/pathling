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

package au.csiro.pathling.search.filter;

import au.csiro.pathling.search.InvalidModifierException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Factory interface for creating search filters for a specific search parameter type.
 * <p>
 * Each search parameter type (TOKEN, STRING, DATE, NUMBER) implements this interface
 * to create the appropriate {@link ElementMatcher} for that type, validate modifiers,
 * and handle FHIR type variations (e.g., Period vs DateTime for DATE).
 * <p>
 * This enables the Open/Closed Principle: new search parameter types can be added
 * by implementing this interface without modifying existing code.
 */
@FunctionalInterface
public interface MatcherFactory {

  /**
   * Creates a search filter for the given modifier and FHIR type.
   *
   * @param modifier the search modifier (e.g., "not", "exact"), or null for no modifier
   * @param fhirType the FHIR type of the element being searched
   * @return a configured SearchFilter
   * @throws InvalidModifierException if the modifier is not supported for this type
   */
  @Nonnull
  SearchFilter createFilter(@Nullable String modifier, @Nonnull FHIRDefinedType fhirType);
}
