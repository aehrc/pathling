package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;

public record CalendarDurationLiteral(String value, String unit) implements FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    return null;
  }
 
}
