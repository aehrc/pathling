package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.EmptyRepresentation;
import au.csiro.pathling.fhirpath.context.FhirPathContext;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.context.ViewEvaluationContext;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Map;
import lombok.Value;
import org.checkerframework.checker.units.qual.N;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value
public class NullEvaluator {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  FhirContext fhirContext;

  @Nonnull
  FunctionRegistry<?> functionRegistry;

  @Nonnull
  Map<String, Collection> variables;


  class UnsupportedResourceResolver implements ResourceResolver {

    @Nonnull
    @Override
    public ResourceCollection resolveResource(@Nonnull final ResourceType resourceType) {
      return ResourceCollection.build(EmptyRepresentation.getInstance(), fhirContext, resourceType);
    }

    @Override
    @Nonnull
    public Collection resolveJoin(
        @Nonnull final ReferenceCollection referenceCollection) {
      throw new UnsupportedOperationException("resolveJoin() is not supported");
    }

    @Nonnull
    @Override
    public ResourceCollection resolveReverseJoin(@Nonnull final ResourceCollection parentResource,
        @Nonnull final String expression) {
      throw new UnsupportedOperationException("resolveReverseJoin() is not supported");
    }
  }

  @Nonnull
  public Collection evaluate(@Nonnull final FhirPath path) {
    final ResourceResolver resourceResolver = new UnsupportedResourceResolver();
    final FhirPathContext fhirpathContext = FhirPathContext.ofResource(
        resourceResolver.resolveResource(subjectResource), variables);
    final EvaluationContext evalContext = new ViewEvaluationContext(fhirpathContext,
        functionRegistry, resourceResolver);
    return path.apply(fhirpathContext.getInputContext(), evalContext);
  }

  @Nonnull
  public static NullEvaluator of(@Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext) {
    return new NullEvaluator(subjectResource, fhirContext, StaticFunctionRegistry.getInstance(),
        Map.of());
  }
}

