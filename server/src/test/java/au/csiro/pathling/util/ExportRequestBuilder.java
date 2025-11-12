package au.csiro.pathling.util;

import static org.hl7.fhir.r4.model.Enumerations.ResourceType;

import au.csiro.pathling.operations.bulkexport.ExportOutputFormat;
import au.csiro.pathling.operations.bulkexport.ExportRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hl7.fhir.r4.model.InstantType;

/**
 * @author Felix Naumann
 */
public class ExportRequestBuilder {

    public static OriginalRequestStep builder() {
        return new BuilderImpl();
    }

    public interface OriginalRequestStep {
        OutputFormatStep originalRequest(String originalRequest);
    }

    public interface OutputFormatStep {
        SinceStep outputFormat(ExportOutputFormat outputFormat);
    }

    public interface SinceStep {
        OptionalStep since(InstantType since);
    }

    public interface OptionalStep {
        OptionalStep until(InstantType until);
        OptionalStep includeResourceType(ResourceType resourceType);
        OptionalStep includeResourceTypes(List<ResourceType> resourceTypes);
        OptionalStep includeResourceTypes(ResourceType... resourceTypes);
        OptionalStep element(String elementName);
        OptionalStep element(ResourceType resourceType, String elementName);
        OptionalStep elements(List<ExportRequest.FhirElement> elements);
        OptionalStep elements(ExportRequest.FhirElement... elements);
        ExportRequest build();
    }

    private static class BuilderImpl implements OriginalRequestStep, OutputFormatStep, SinceStep, OptionalStep {
        private String originalRequest;
        private ExportOutputFormat outputFormat;
        private InstantType since;
        private InstantType until;
        private final List<ResourceType> includeResourceTypeFilters = new ArrayList<>();
        private final List<ExportRequest.FhirElement> elements = new ArrayList<>();

        @Override
        public OutputFormatStep originalRequest(String originalRequest) {
            this.originalRequest = originalRequest;
            return this;
        }

        @Override
        public SinceStep outputFormat(ExportOutputFormat outputFormat) {
            this.outputFormat = outputFormat;
            return this;
        }

        @Override
        public OptionalStep since(InstantType since) {
            this.since = since;
            return this;
        }

        @Override
        public OptionalStep until(InstantType until) {
            this.until = until;
            return this;
        }

        @Override
        public OptionalStep includeResourceType(ResourceType resourceType) {
            this.includeResourceTypeFilters.add(resourceType);
            return this;
        }

        @Override
        public OptionalStep includeResourceTypes(List<ResourceType> resourceTypes) {
            this.includeResourceTypeFilters.addAll(resourceTypes);
            return this;
        }

        @Override
        public OptionalStep includeResourceTypes(ResourceType... resourceTypes) {
            this.includeResourceTypeFilters.addAll(Arrays.asList(resourceTypes));
            return this;
        }

        @Override
        public OptionalStep element(String elementName) {
            this.elements.add(new ExportRequest.FhirElement(null, elementName));
            return this;
        }

        @Override
        public OptionalStep element(ResourceType resourceType, String elementName) {
            this.elements.add(new ExportRequest.FhirElement(resourceType, elementName));
            return this;
        }

        @Override
        public OptionalStep elements(List<ExportRequest.FhirElement> elements) {
            this.elements.addAll(elements);
            return this;
        }

        @Override
        public OptionalStep elements(ExportRequest.FhirElement... elements) {
            this.elements.addAll(Arrays.asList(elements));
            return this;
        }

        @Override
        public ExportRequest build() {
            return new ExportRequest(
                    originalRequest,
                    outputFormat,
                    since,
                    until,
                    List.copyOf(includeResourceTypeFilters),
                    List.copyOf(elements)
            );
        }
    }
}
