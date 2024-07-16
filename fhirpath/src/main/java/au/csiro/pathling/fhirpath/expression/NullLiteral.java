package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;

public record NullLiteral() implements FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    return new EmptyCollection();
  }

}
