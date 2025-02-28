package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a definition of a FHIR-like resource.
 */
public interface ResourceDefinition extends NodeDefinition {

  @Nonnull
  ResourceTag getResourceTag();

  @Nonnull
  default String getResourceCode() {
    return getResourceTag().toCode();
  }

  @Deprecated
  @Nonnull
  ResourceType getResourceType();
}
