package au.csiro.pathling.util;

import au.csiro.pathling.operations.bulkexport.ExportOutputFormat;
import au.csiro.pathling.operations.bulkexport.ExportRequest;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
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

    OptionalStep includeResourceType(String resourceTypeCode);

    OptionalStep includeResourceTypes(List<String> resourceTypeCodes);

    OptionalStep includeResourceTypes(String... resourceTypeCodes);

    OptionalStep element(String elementName);

    OptionalStep element(String resourceTypeCode, String elementName);

    OptionalStep elements(List<ExportRequest.FhirElement> elements);

    OptionalStep elements(ExportRequest.FhirElement... elements);

    ExportRequest build();
  }

  private static class BuilderImpl
      implements OriginalRequestStep, OutputFormatStep, SinceStep, OptionalStep {

    private String originalRequest;
    private ExportOutputFormat outputFormat;
    private InstantType since;
    private InstantType until;
    private final List<String> includeResourceTypeFilters = new ArrayList<>();
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
      this.includeResourceTypeFilters.add(resourceType.toCode());
      return this;
    }

    @Override
    public OptionalStep includeResourceType(String resourceTypeCode) {
      this.includeResourceTypeFilters.add(resourceTypeCode);
      return this;
    }

    @Override
    public OptionalStep includeResourceTypes(List<String> resourceTypeCodes) {
      this.includeResourceTypeFilters.addAll(resourceTypeCodes);
      return this;
    }

    @Override
    public OptionalStep includeResourceTypes(String... resourceTypeCodes) {
      this.includeResourceTypeFilters.addAll(Arrays.asList(resourceTypeCodes));
      return this;
    }

    @Override
    public OptionalStep element(String elementName) {
      this.elements.add(new ExportRequest.FhirElement(null, elementName));
      return this;
    }

    @Override
    public OptionalStep element(String resourceTypeCode, String elementName) {
      this.elements.add(new ExportRequest.FhirElement(resourceTypeCode, elementName));
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
      // Derive server base URL from original request for test convenience.
      String serverBaseUrl = originalRequest;
      final int exportIndex = originalRequest.indexOf("$export");
      if (exportIndex > 0) {
        serverBaseUrl = originalRequest.substring(0, exportIndex);
        if (serverBaseUrl.endsWith("/")) {
          serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
        }
      }
      return new ExportRequest(
          originalRequest,
          serverBaseUrl,
          outputFormat,
          since,
          until,
          List.copyOf(includeResourceTypeFilters),
          List.copyOf(elements),
          false,
          ExportLevel.SYSTEM,
          Set.of());
    }
  }
}
