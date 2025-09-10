package au.csiro.pathling.export;

import ca.uhn.fhir.rest.client.method.ElementsParameter;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.InstantType;

import java.util.List;

/**
 * @author Felix Naumann
 */
public record ExportRequest(
        @Nonnull String originalRequest,
        @Nonnull ExportOutputFormat outputFormat,
        @Nonnull InstantType since,
        @Nullable InstantType until,
        @Nonnull List<ResourceType> includeResourceTypeFilters,
        @Nonnull List<FhirElement> elements
) {
        public ExportRequest(@Nonnull String originalRequest, @Nonnull ExportOutputFormat outputFormat, @Nonnull InstantType since) {
                this(originalRequest, outputFormat, since, null, List.of(), List.of());
        }

        public record FhirElement(@Nullable ResourceType resourceType, @Nonnull String elementName) {}
}
