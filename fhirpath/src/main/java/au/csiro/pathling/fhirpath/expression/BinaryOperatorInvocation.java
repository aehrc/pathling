package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;

public record BinaryOperatorInvocation(FhirPath left, FhirPath right, String operator) implements
    FhirPath {

  @Override
  public Collection apply(final Collection input, final EvaluationContext context) {
    return null;
  }

}
