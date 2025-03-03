package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a definition of a FHIR-like reference element.
 */
public interface ReferenceDefinition extends ElementDefinition {
  
  /**
   * Returns the set of resources that a reference can refer to.
   *
   * @return A set of {@link ResourceType} objects, if this element is a reference
   */
  @Nonnull
  ResourceTypeSet getReferenceTypes();
}
