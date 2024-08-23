package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;

public record Invocation(FhirPath source, FhirPath target) implements FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    final Collection sourceResult = source.evaluate(input, context);
    return target.evaluate(sourceResult, context);
  }

}
