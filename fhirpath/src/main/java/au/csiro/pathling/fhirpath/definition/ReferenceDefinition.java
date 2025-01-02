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

package au.csiro.pathling.fhirpath.definition;

import static java.util.Objects.requireNonNull;

import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import jakarta.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Reference;

/**
 * Encapsulates the FHIR definitions for a reference element.
 *
 * @author John Grimes
 */
public class ReferenceDefinition extends ElementChildDefinition {


  private final List<Class<? extends IBaseResource>> resourceTypes;


  protected ReferenceDefinition(@Nonnull final RuntimeChildChoiceDefinition childDefinition,
      @Nonnull final String elementName) {
    super(requireNonNull(childDefinition.getChildElementDefinitionByDatatype(Reference.class)),
        childDefinition,
        elementName);
    resourceTypes = childDefinition.getResourceTypes();
    requireNonNull(resourceTypes);
  }

  protected ReferenceDefinition(@Nonnull final RuntimeChildResourceDefinition childDefinition) {
    super(childDefinition);
    resourceTypes = childDefinition.getResourceTypes();
    requireNonNull(resourceTypes);
  }

  /**
   * Returns the set of resources that a reference can refer to.
   *
   * @return A set of {@link ResourceType} objects, if this element is a reference
   */
  @Nonnull
  public ResourceTypeSet getReferenceTypes() {
    // final List<Class<? extends IBaseResource>> resourceTypes = ((RuntimeChildResourceDefinition) childDefinition).getResourceTypes();
    // requireNonNull(resourceTypes);
    return ResourceTypeSet.from(resourceTypes.stream()
        .map(clazz -> {
          final String resourceCode;
          try {
            if (clazz.getName().equals("org.hl7.fhir.instance.model.api.IAnyResource")) {
              return Enumerations.ResourceType.RESOURCE;
            } else {
              resourceCode = clazz.getConstructor().newInstance().fhirType();
              return Enumerations.ResourceType.fromCode(resourceCode);
            }
          } catch (final InstantiationException | IllegalAccessException |
                         InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Problem accessing resource types on element", e);
          }
        })
        .collect(Collectors.toSet()));
  }

  public static boolean isReferenceDefinition(
      @Nonnull final BaseRuntimeElementDefinition<?> elementDefinition) {
    return FHIRDefinedType.REFERENCE.toCode().equals(elementDefinition.getName());
  }

}
