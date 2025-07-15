package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a definition of a FHIR-like element.
 */
public interface ElementDefinition extends ChildDefinition {

  /**
   * @return the name of this element
   */
  @Nonnull
  String getElementName();

  /**
   * @return The {@link FHIRDefinedType} that corresponds to the type of this element. Not all
   * elements have a type, e.g. polymorphic elements.
   */
  @Nonnull
  Optional<FHIRDefinedType> getFhirType();
  
  /**
   * @return true if this element is a choice element, false otherwise
   */
  default boolean isChoiceElement() {
    return false;
  }
}
