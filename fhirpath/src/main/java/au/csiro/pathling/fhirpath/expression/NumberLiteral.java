package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;

public record NumberLiteral(String value) implements FhirPath {

  @Override
  public Collection apply(final Collection input, final EvaluationContext context) {
    return null;
  }
 
}
