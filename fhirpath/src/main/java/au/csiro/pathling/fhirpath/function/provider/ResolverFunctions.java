package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResolverFunctions {

  @FhirPathFunction
  @Nonnull
  public static ResourceCollection reverseResolve(@Nonnull final ResourceCollection input,
      @Nonnull final FhirPath subjectPath, @Nonnull EvaluationContext evaluationContext) {
    // subject path should be Resource + traversal path
    final ReverseResolveRoot root = ReverseResolveRoot.fromChildPath(
        ResourceRoot.of(input.getResourceType()),
        subjectPath);
    log.debug("Reverse resolve root: {}", root);
    return evaluationContext.resolveReverseJoin(input, root.getForeignResourceType().toCode(),
        root.getForeignKeyPath());
  }

  @FhirPathFunction
  @Nonnull
  public static Collection resolve(@Nonnull final ReferenceCollection input,
      @Nonnull EvaluationContext evaluationContext) {
    return evaluationContext.resolveJoin(input);
  }
}
