package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildExtension;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;


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
    // TODO: check why this is safe to remove
    // if (childDefinition instanceof RuntimeChildAny && "valueReference".equals(childDefinition
    //     .getElementName())) {
    //   return new ReferenceExtensionDefinition((RuntimeChildAny) childDefinition);
    // } else 
    if (childDefinition instanceof RuntimeChildResourceDefinition) {
      return new ReferenceDefinition((RuntimeChildResourceDefinition) childDefinition);
    } else if (childDefinition instanceof RuntimeChildChoiceDefinition
        && !(childDefinition instanceof RuntimeChildExtension)) {
      return new ChoiceChildDefinition((RuntimeChildChoiceDefinition) childDefinition);
    } else {
      return new ElementChildDefinition(childDefinition);
    }
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
