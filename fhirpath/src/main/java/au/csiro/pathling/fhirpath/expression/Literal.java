package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;

public record Literal(FhirPathType type, String expression) implements FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    return null;
  }
 
}
