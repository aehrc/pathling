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
package au.csiro.pathling.fhirpath;

import au.csiro.pathling.definition.FhirType;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Resolves a type code to a {@link FhirType}, rejecting a code that the version of FHIR this engine
 * implements does not describe.
 *
 * <p>The definitions themselves report a type code without reference to any particular version of
 * FHIR, but a type name that reaches the engine from an expression or a view definition is user
 * input, and is validated here before it is used.
 */
public final class FhirTypes {

  private FhirTypes() {}

  /**
   * Resolves a type code against the types FHIR R4 describes.
   *
   * @param code the type code to resolve
   * @return the corresponding type, or empty if R4 describes no type with this code
   */
  @Nonnull
  public static Optional<FhirType> resolve(@Nonnull final String code) {
    try {
      return Optional.ofNullable(FHIRDefinedType.fromCode(code)).map(type -> FhirType.of(code));
    } catch (final FHIRException e) {
      // A code that the enumeration does not contain does not resolve to a type.
      return Optional.empty();
    }
  }
}
