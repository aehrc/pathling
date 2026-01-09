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

package au.csiro.pathling.search;

import jakarta.annotation.Nonnull;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Enum representing the types of FHIR search parameters.
 * <p>
 * Each search parameter type has a set of allowed FHIR types that can be searched using that
 * parameter type.
 *
 * @see <a href="https://hl7.org/fhir/search.html#ptypes">Search Parameter Types</a>
 */
public enum SearchParameterType {

  /**
   * A token type search parameter matches a system and/or code.
   */
  TOKEN(Set.of(
      FHIRDefinedType.CODE, FHIRDefinedType.CODING, FHIRDefinedType.CODEABLECONCEPT,
      FHIRDefinedType.IDENTIFIER, FHIRDefinedType.CONTACTPOINT,
      FHIRDefinedType.BOOLEAN, FHIRDefinedType.STRING, FHIRDefinedType.URI, FHIRDefinedType.ID
  )),

  /**
   * A string type search parameter matches string values.
   */
  STRING(Set.of(
      FHIRDefinedType.STRING, FHIRDefinedType.HUMANNAME, FHIRDefinedType.ADDRESS,
      FHIRDefinedType.MARKDOWN
  )),

  /**
   * A date type search parameter matches date/time values.
   */
  DATE(Set.of(
      FHIRDefinedType.DATE, FHIRDefinedType.DATETIME, FHIRDefinedType.INSTANT,
      FHIRDefinedType.PERIOD
  )),

  /**
   * A quantity type search parameter matches quantity values with optional units.
   */
  QUANTITY(Set.of()),

  /**
   * A reference type search parameter matches references to other resources.
   */
  REFERENCE(Set.of()),

  /**
   * A number type search parameter matches numeric values.
   */
  NUMBER(Set.of(
      FHIRDefinedType.INTEGER, FHIRDefinedType.DECIMAL,
      FHIRDefinedType.POSITIVEINT, FHIRDefinedType.UNSIGNEDINT
  )),

  /**
   * A URI type search parameter matches URI values.
   */
  URI(Set.of()),

  /**
   * A composite type search parameter combines multiple parameters.
   */
  COMPOSITE(Set.of()),

  /**
   * A special type search parameter has custom behavior.
   */
  SPECIAL(Set.of());

  @Nonnull
  private final Set<FHIRDefinedType> allowedFhirTypes;

  SearchParameterType(@Nonnull final Set<FHIRDefinedType> allowedFhirTypes) {
    this.allowedFhirTypes = allowedFhirTypes;
  }

  /**
   * Checks if the given FHIR type is allowed for this search parameter type.
   *
   * @param fhirType the FHIR type to check
   * @return true if the type is allowed, false otherwise
   */
  public boolean isAllowedFhirType(@Nonnull final FHIRDefinedType fhirType) {
    return allowedFhirTypes.contains(fhirType);
  }

  /**
   * Gets the set of FHIR types that are allowed for this search parameter type.
   *
   * @return the set of allowed FHIR types
   */
  @Nonnull
  public Set<FHIRDefinedType> getAllowedFhirTypes() {
    return allowedFhirTypes;
  }
}
