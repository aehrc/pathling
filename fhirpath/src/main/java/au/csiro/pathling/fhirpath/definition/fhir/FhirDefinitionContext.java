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

package au.csiro.pathling.fhirpath.definition.fhir;

import static au.csiro.pathling.fhirpath.definition.fhir.FhirReferenceDefinition.isReferenceDefinition;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildExtension;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * The definition context that encapsulates a {@link FhirContext} and provides access to all FHIR
 * resource definitions within it.
 */
@Value(staticConstructor = "of")
public class FhirDefinitionContext implements DefinitionContext {

  @Nonnull FhirContext fhirContext;

  @Override
  @Nonnull
  public ResourceDefinition findResourceDefinition(@Nonnull final String resourceCode) {
    // Try to resolve to a standard ResourceType, but allow custom types (like ViewDefinition).
    final Optional<ResourceType> resourceType = tryGetResourceType(resourceCode);
    final RuntimeResourceDefinition hapiDefinition =
        fhirContext.getResourceDefinition(resourceCode);
    return new FhirResourceDefinition(resourceCode, resourceType, requireNonNull(hapiDefinition));
  }

  /**
   * Finds the resource definition for the given {@link ResourceType}.
   *
   * @param resourceType the FHIR resource type to find the definition for
   * @return the corresponding {@link ResourceDefinition} for the resource type
   */
  @Nonnull
  public ResourceDefinition findResourceDefinition(@Nonnull final ResourceType resourceType) {
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition =
        fhirContext.getResourceDefinition(resourceCode);
    return new FhirResourceDefinition(resourceCode, Optional.of(resourceType), requireNonNull(hapiDefinition));
  }

  /**
   * Attempts to resolve a resource code to a standard FHIR ResourceType.
   *
   * @param resourceCode the resource code to resolve
   * @return the ResourceType if it's a standard type, otherwise empty
   */
  @Nonnull
  private static Optional<ResourceType> tryGetResourceType(@Nonnull final String resourceCode) {
    try {
      return Optional.of(ResourceType.fromCode(resourceCode));
    } catch (final org.hl7.fhir.exceptions.FHIRException e) {
      // Custom resource type not in the standard FHIR specification.
      return Optional.empty();
    }
  }

  /**
   * Builds an ElementDefinition from a HAPI child definition.
   *
   * @param childDefinition A HAPI {@link BaseRuntimeChildDefinition} that describes this element
   * @param elementName the name of the element
   * @return A shiny new ElementDefinition
   */
  @Nonnull
  static Optional<ChildDefinition> build(
      @Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {

    if (isChildChoiceDefinition(childDefinition)
        && childDefinition.getElementName().equals(elementName)) {
      return Optional.of(new FhirChoiceDefinition((RuntimeChildChoiceDefinition) childDefinition));
    } else {
      return buildElement(childDefinition, elementName).map(e -> e);
    }
  }

  @Nonnull
  static Optional<ElementDefinition> buildElement(
      @Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {

    if (childDefinition.getValidChildNames().contains(elementName)) {
      final BaseRuntimeElementDefinition<?> elementDefinition =
          childDefinition.getChildByName(elementName);
      if (childDefinition instanceof final RuntimeChildResourceDefinition rctd) {
        return Optional.of(new FhirReferenceDefinition(rctd));
      } else if (isChildChoiceDefinition(childDefinition)
          && isReferenceDefinition(elementDefinition)) {
        return Optional.of(
            new FhirReferenceDefinition(
                (RuntimeChildChoiceDefinition) childDefinition, elementName));
      } else if (nonNull(elementDefinition)) {
        return Optional.of(new FhirElementDefinition(childDefinition, elementName));
      }
    }
    return Optional.empty();
  }

  static boolean isChildChoiceDefinition(
      @Nonnull final BaseRuntimeChildDefinition childDefinition) {
    return childDefinition instanceof RuntimeChildChoiceDefinition
        && !(childDefinition instanceof RuntimeChildExtension);
  }
}
