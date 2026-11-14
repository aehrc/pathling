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

package au.csiro.pathling.test.helpers;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.evaluation.ElementRenderer;
import au.csiro.pathling.io.FhirWriter;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import lombok.experimental.UtilityClass;

/**
 * Supplies single-resource evaluation in tests with the renderer that {@code PathlingContext}
 * passes it in production, the layout-to-JSON transform of the {@code io} module (decision 91).
 *
 * @author Piotr Szul
 */
@UtilityClass
public class JsonElementRenderer {

  /**
   * Returns the renderer of {@code io}'s JSON writer for the definitions of a FHIR context.
   *
   * @param fhirContext the FHIR context
   * @return the renderer
   */
  @Nonnull
  public static ElementRenderer of(@Nonnull final FhirContext fhirContext) {
    return FhirWriter.of(FhirDefinitionContext.of(fhirContext)).json()::writeElement;
  }
}
