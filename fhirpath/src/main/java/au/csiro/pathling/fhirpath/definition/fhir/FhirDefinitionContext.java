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
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.ListResource;

/**
 * The definition context that encapsulates a {@link FhirContext} and provides access to all FHIR
 * resource definitions within it.
 */
@Value(staticConstructor = "of")
public class FhirDefinitionContext implements DefinitionContext {

  @Nonnull
  FhirContext fhirContext;

  /**
   * Returns the {@link ResourceType} for a HAPI resource class.
   *
   * @param resourceClass the resource class, extending {@link IBaseResource}
   * @return a {@link ResourceType} value
   * @throws IllegalArgumentException if the resource type was not found
   */
  @Nonnull
  public static ResourceType getResourceTypeFromClass(
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    // List is the only resource for which the HAPI resource class name does not match the resource
    // code.
    final String resourceName = resourceClass == ListResource.class
                                ? "List"
                                : resourceClass.getSimpleName();
    return ResourceType.fromCode(resourceName);
  }


  @Override
  @Nonnull
  public ResourceDefinition findResourceDefinition(@Nonnull final String resourceCode) {
    final ResourceType resourceType = ResourceType.fromCode(resourceCode);
    final RuntimeResourceDefinition hapiDefinition = fhirContext.getResourceDefinition(
        resourceCode);
    return new FhirResourceDefinition(resourceType, requireNonNull(hapiDefinition));
  }

  @Nonnull
  public ResourceDefinition findResourceDefinition(@Nonnull final ResourceType resourceType) {
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext.getResourceDefinition(
        resourceCode);
    return new FhirResourceDefinition(resourceType, requireNonNull(hapiDefinition));
  }


  /**
   * @param childDefinition A HAPI {@link BaseRuntimeChildDefinition} that describes this element
   * @return A shiny new ElementDefinition
   */
  @Nonnull
  static Optional<? extends ChildDefinition> build(
      @Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {

    if (isChildChoiceDefinition(childDefinition) && childDefinition.getElementName()
        .equals(elementName)) {
      return Optional.of(
          new FhirChoiceDefinition((RuntimeChildChoiceDefinition) childDefinition));
    } else {
      return buildElement(childDefinition, elementName);
    }
  }

  @Nonnull
  static Optional<ElementDefinition> buildElement(
      @Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {

    if (childDefinition.getValidChildNames().contains(elementName)) {
      final BaseRuntimeElementDefinition<?> elementDefinition = childDefinition.getChildByName(
          elementName);
      if (childDefinition instanceof RuntimeChildResourceDefinition rctd) {
        return Optional.of(new FhirReferenceDefinition(rctd));
      } else if (isChildChoiceDefinition(childDefinition) && isReferenceDefinition(
          elementDefinition)) {
        return Optional.of(
            new FhirReferenceDefinition((RuntimeChildChoiceDefinition) childDefinition,
                elementName));
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
