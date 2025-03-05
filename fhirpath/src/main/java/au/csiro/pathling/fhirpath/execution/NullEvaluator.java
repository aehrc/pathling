package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@UtilityClass
public class NullEvaluator {

  @EqualsAndHashCode(callSuper = true)
  @Value
  static class NullResourceResolver extends BaseResourceResolver {

    @Nonnull
    ResourceType subjectResource;
    @Nonnull
    FhirContext fhirContext;

    @Nonnull
    protected ResourceCollection createResource(@Nonnull final ResourceType resourceType) {
      return ResourceCollection.build(
          DefaultRepresentation.empty(),
          getFhirContext(), resourceType);
    }
  }

  @Nonnull
  public static StdFhirpathEvaluator of(@Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext) {
    return new StdFhirpathEvaluator(
        new NullResourceResolver(subjectResource, fhirContext),
        StaticFunctionRegistry.getInstance(),
        Map.of());
  }
}

