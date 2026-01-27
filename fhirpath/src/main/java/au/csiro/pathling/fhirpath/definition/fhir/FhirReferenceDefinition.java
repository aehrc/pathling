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

package au.csiro.pathling.fhirpath.definition.fhir;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Reference;

/**
 * Encapsulates the FHIR definitions for a reference element.
 *
 * @author John Grimes
 */
class FhirReferenceDefinition extends FhirElementDefinition implements ElementDefinition {

  private final List<Class<? extends IBaseResource>> resourceTypes;

  protected FhirReferenceDefinition(
      @Nonnull final RuntimeChildChoiceDefinition childDefinition,
      @Nonnull final String elementName) {
    super(
        requireNonNull(childDefinition.getChildElementDefinitionByDatatype(Reference.class)),
        childDefinition,
        elementName);
    resourceTypes = childDefinition.getResourceTypes();
    requireNonNull(resourceTypes);
  }

  protected FhirReferenceDefinition(@Nonnull final RuntimeChildResourceDefinition childDefinition) {
    super(childDefinition);
    resourceTypes = childDefinition.getResourceTypes();
    requireNonNull(resourceTypes);
  }

  public static boolean isReferenceDefinition(
      @Nonnull final BaseRuntimeElementDefinition<?> elementDefinition) {
    return FHIRDefinedType.REFERENCE.toCode().equals(elementDefinition.getName());
  }
}
