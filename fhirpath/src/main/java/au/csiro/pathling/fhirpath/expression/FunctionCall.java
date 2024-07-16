package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import java.util.List;

public record FunctionCall(String name, List<FhirPath> arguments) implements FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    return null;
  }

}
