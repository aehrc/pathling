package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import org.apache.commons.lang3.NotImplementedException;

public record ExternalConstant(String name) implements FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    throw new NotImplementedException("External constants are not yet implemented");
  }

}
