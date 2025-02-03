package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public interface ResourceDefinition extends NodeDefinition {

  @Nonnull
  ResourceType getResourceType();
}
