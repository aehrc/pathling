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

package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a definition of a FHIR-like element.
 */
public interface ElementDefinition extends ChildDefinition {

  /**
   * @return the name of this element
   */
  @Nonnull
  String getElementName();

  /**
   * @return The {@link FHIRDefinedType} that corresponds to the type of this element. Not all
   * elements have a type, e.g. polymorphic elements.
   */
  @Nonnull
  Optional<FHIRDefinedType> getFhirType();
  
  /**
   * @return true if this element is a choice element, false otherwise
   */
  default boolean isChoiceElement() {
    return false;
  }
}
