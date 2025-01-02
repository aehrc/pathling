package au.csiro.pathling.fhirpath.definition;

import static au.csiro.pathling.fhirpath.definition.ReferenceDefinition.isReferenceDefinition;
import static java.util.Objects.nonNull;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildExtension;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;


/**
 * Encapsulates the FHIR definitions for a child.
 *
 * @author Piotr Szul
 */
public interface ChildDefinition extends NodeDefinition {

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
          new ChoiceChildDefinition((RuntimeChildChoiceDefinition) childDefinition));
    } else {
      return buildElement(childDefinition, elementName);
    }
  }

  @Nonnull
  static Optional<ElementDefinition> buildElement(
      @Nonnull final BaseRuntimeChildDefinition childDefinition,
      @Nonnull final String elementName) {
    final BaseRuntimeElementDefinition<?> elementDefinition = childDefinition.getChildByName(
        elementName);
    if (childDefinition instanceof RuntimeChildResourceDefinition rctd) {
      return Optional.of(new ReferenceDefinition(rctd));
    } else if (isChildChoiceDefinition(childDefinition) && isReferenceDefinition(
        elementDefinition)) {
      return Optional.of(
          new ReferenceDefinition((RuntimeChildChoiceDefinition) childDefinition, elementName));
    } else if (nonNull(elementDefinition)) {
      return Optional.of(new ElementChildDefinition(childDefinition, elementName));
    } else {
      return Optional.empty();
    }
  }


  static boolean isChildChoiceDefinition(
      @Nonnull final BaseRuntimeChildDefinition childDefinition) {
    return childDefinition instanceof RuntimeChildChoiceDefinition
        && !(childDefinition instanceof RuntimeChildExtension);
  }

  /**
   * @return the name of this child
   */
  @Nonnull
  String getName();

  /**
   * @return The maximum cardinality for this child
   */
  @Nonnull
  Optional<Integer> getMaxCardinality();

}
