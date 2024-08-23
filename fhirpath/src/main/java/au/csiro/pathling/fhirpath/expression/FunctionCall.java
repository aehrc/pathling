package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;

public record FunctionCall(String name, List<FhirPath> arguments) implements FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    throw new NotImplementedException("Function calls are not yet implemented");
  }

}
