package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
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
  static ChildDefinition build(@Nonnull final BaseRuntimeChildDefinition childDefinition) {
    // TODO: Check if this is safe to remove.
    //   if (childDefinition instanceof RuntimeChildAny && "valueReference".equals(childDefinition
    //       .getElementName())) {
    //     return new ReferenceExtensionDefinition((RuntimeChildAny) childDefinition);
    //   } else 
    if (childDefinition instanceof RuntimeChildResourceDefinition) {
      return new ReferenceDefinition((RuntimeChildResourceDefinition) childDefinition);
    } else if (isChildChoiceDefinition(childDefinition)) {
      return new ChoiceChildDefinition((RuntimeChildChoiceDefinition) childDefinition);
    } else {
      return new ElementChildDefinition(childDefinition);
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
