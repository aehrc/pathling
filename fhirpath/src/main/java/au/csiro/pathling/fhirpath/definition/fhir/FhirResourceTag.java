package au.csiro.pathling.fhirpath.definition.fhir;

import au.csiro.pathling.fhirpath.definition.ResourceTag;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIR resource tag.
 */
@Value(staticConstructor = "of")
public class FhirResourceTag implements ResourceTag {

  @Nonnull
  ResourceType resourceType;

  @Override
  @Nonnull
  public String toString() {
    return resourceType.toString();
  }

  @Override
  @Nonnull
  public String toCode() {
    return resourceType.toCode();
  }
}
