package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import java.util.Optional;


/**
 * Represents a choice child elements in a FHIR-like schema.
 */
public interface ChoiceDefinition extends ChildDefinition {

  /**
   * Returns the child element definition for the given type, if it exists.
   *
   * @param type the type of the child element
   * @return the child element definition, if it exists
   */
  @Nonnull
  Optional<ElementDefinition> getChildByType(@Nonnull final String type);
}
