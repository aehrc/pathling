package au.csiro.pathling.fhir;

import jakarta.annotation.Nullable;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A utility class for working with FHIR resources.
 *
 * @author John Grimes
 */
public class FhirUtils {

  /**
   * A method for getting a FHIR resource type from a string. This method will always throw an error
   * if the string does not match a known resource type.
   *
   * @param resourceCode the string to convert to a resource type
   * @return the resource type
   */
  public static ResourceType getResourceType(@Nullable final String resourceCode) {
    @Nullable final ResourceType resourceType = ResourceType.fromCode(resourceCode);
    if (resourceType == null) {
      throw new FHIRException("Unknown resource type: " + resourceCode);
    }
    return resourceType;
  }

}
