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

package au.csiro.pathling.definition;

import jakarta.annotation.Nonnull;
import java.util.Optional;

/** Represents a definition of a FHIR-like element. */
public interface ElementDefinition extends ChildDefinition {

  /**
   * Gets the name of this element.
   *
   * @return the name of this element
   */
  @Nonnull
  String getElementName();

  /**
   * Gets the FHIR type of this element.
   *
   * @return The {@link FhirType} that corresponds to the type of this element. Not all elements
   *     have a type, e.g. polymorphic elements.
   */
  @Nonnull
  Optional<FhirType> getFhirType();

  /**
   * Gets the maximum number of values this element may hold, where a negative value indicates that
   * it is unbounded.
   *
   * @return the maximum cardinality of this element
   */
  int getMaxCardinality();

  /**
   * Checks whether this element may hold more than one value, and is therefore stored as an array
   * rather than as a scalar.
   *
   * @return true if this element may repeat, false otherwise
   */
  default boolean isRepeating() {
    return getMaxCardinality() < 0 || getMaxCardinality() > 1;
  }

  /**
   * Checks if this element is a choice element.
   *
   * @return true if this element is a choice element, false otherwise
   */
  default boolean isChoiceElement() {
    return false;
  }
}
