/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.fhirpath.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
public class FhirHelpers {

  @Nonnull
  public static Optional<? extends ChildDefinition> getChildOfResource(
      @Nonnull final FhirContext fhirContext, @Nonnull final String resourceCode,
      @Nonnull final String elementName) {
    final ResourceDefinition definition = FhirDefinitionContext.of(fhirContext)
        .findResourceDefinition(resourceCode);
    return definition.getChildElement(elementName);
  }

}
