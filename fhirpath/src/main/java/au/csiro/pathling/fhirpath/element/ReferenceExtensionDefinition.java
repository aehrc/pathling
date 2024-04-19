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

package au.csiro.pathling.fhirpath.element;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import jakarta.annotation.Nonnull;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Encapsulates the FHIR definitions for a Reference-typed extension.
 *
 * @author John Grimes
 */
public class ReferenceExtensionDefinition extends ElementDefinition {

  protected ReferenceExtensionDefinition(@Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {
    super(childDefinition, elementName);
  }

  @Override
  @Nonnull
  public Set<ResourceType> getReferenceTypes() {
    // We always treat a reference extension as a reference to any resource, as we don't have enough 
    // information to constrain it further.
    return Set.of(ResourceType.RESOURCE);
  }

}
