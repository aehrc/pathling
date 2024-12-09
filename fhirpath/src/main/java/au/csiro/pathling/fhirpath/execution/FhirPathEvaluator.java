package au.csiro.pathling.fhirpath.execution;

import jakarta.annotation.Nonnull;

public interface FhirPathEvaluator {


  /**
   * Evaluate the given FHIRPath expression.
   *
   * @param fhirpathExpression the FHIRPath expression to evaluate
   * @return the result of the evaluation
   */
  @Nonnull
  CollectionDataset evaluate(@Nonnull final String fhirpathExpression);

}
