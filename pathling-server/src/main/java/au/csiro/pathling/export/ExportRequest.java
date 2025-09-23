package au.csiro.pathling.export;

import ca.uhn.fhir.rest.client.method.ElementsParameter;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.InstantType;

import java.util.List;

/**
 * Parsed data of the incoming export request.
 * 
 * @param originalRequest The original request URL.
 * @param outputFormat The desired output format.
 * @param since Resources will be included in the response if their state has changed after the supplied time.
 * @param until Resources will be included in the response if their state has changed before the supplied time.
 * @param includeResourceTypeFilters When provided, resources will be included in the response if their resource type is listed here.
 * @param elements When provided, the listed FHIR resource elements will be the only ones returned in the resources (alongside mandatory elements).
 * 
 * @author Felix Naumann
 */
public record ExportRequest(
        @Nonnull String originalRequest,
        @Nonnull ExportOutputFormat outputFormat,
        @Nullable InstantType since,
        @Nullable InstantType until,
        @Nonnull List<ResourceType> includeResourceTypeFilters,
        @Nonnull List<FhirElement> elements
) {

  /**
   * A small container for resource types and their top level elements.
   * 
   * @param resourceType The resource type for this element.
   * @param elementName The top level element name of the resource or the top level name across all resources if "resourceType" is null.
   */
  public record FhirElement(@Nullable ResourceType resourceType, @Nonnull String elementName) {}
}
