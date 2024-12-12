package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.fhirpath.path.Paths.Resource;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Slf4j
public class ResolverFunctions {


  @FhirPathFunction
  @Nonnull
  public static ResourceCollection reverseResolve(@Nonnull final ResourceCollection input,
      @Nonnull final FhirPath subjectPath, @Nonnull EvaluationContext evaluationContext) {

    // subject path should be TypeSpecifierPath + traversal path

    final ResourceType childResourceType = ((Resource) subjectPath.first()).getResourceType();
    final String childPath = subjectPath.suffix().toExpression();
    final ReverseResolveRoot root = ReverseResolveRoot.ofResource(input.getResourceType(),
        childResourceType, childPath);

    log.info("Reverse resolve root: {}", root);
    System.out.println("Reverse resolve root: " + root);

    return evaluationContext.resolveReverseJoin(input, subjectPath.toExpression());
  }

}
