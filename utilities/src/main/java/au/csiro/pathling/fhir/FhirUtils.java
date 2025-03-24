package au.csiro.pathling.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import jakarta.annotation.Nonnull;
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

  /**
   * A method for checking if a string is a known FHIR resource type in a given FHIR context.
   *
   * @param resourceCode the string to check
   * @param fhirContext the FHIR context to check against
   * @return true if the string is a known resource type, false otherwise
   */
  public static boolean isKnownResource(@Nullable final String resourceCode, @Nonnull final
  FhirContext fhirContext) {
    try {
      fhirContext.getResourceDefinition(resourceCode);
      return true;
    } catch (DataFormatException ignored) {
      return false;
    }
  }
}
