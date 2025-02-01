package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@Value(staticConstructor = "of")
public class FhirpathExecutor {


  @Nonnull
  Parser parser;

  @Nonnull
  FhirpathEvaluator.Provider provider;

  /**
   * Evaluate the given FHIRPath expression.
   *
   * @param fhirpathExpression the FHIRPath expression to evaluate
   * @return the result of the evaluation
   */
  @Nonnull
  public CollectionDataset evaluate(@Nonnull final ResourceType subjectResource,
      @Nonnull final String fhirpathExpression) {
    final FhirPath fhirpath = parser.parse(fhirpathExpression);
    final FhirpathEvaluator evaluator = provider.create(subjectResource, () -> List.of(fhirpath));
    return CollectionDataset.of(evaluator.createInitialDataset(), evaluator.evaluate(fhirpath));
  }
  
}
