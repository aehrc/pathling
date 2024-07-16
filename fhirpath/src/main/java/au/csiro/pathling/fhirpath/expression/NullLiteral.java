package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;

public record NullLiteral() implements FhirPath {

  @Override
  public Collection apply(final Collection input, final EvaluationContext context) {
    return new EmptyCollection();
  }

}
