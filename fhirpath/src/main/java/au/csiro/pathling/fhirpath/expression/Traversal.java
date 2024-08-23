package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import java.util.Optional;

public record Traversal(Optional<FhirPath> source, String target) implements FhirPath {

  @Override
  public Collection evaluate(final Collection input, final EvaluationContext context) {
    return input.traverse(target);
  }

}
