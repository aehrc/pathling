package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;

public record NumberLiteral(String value) implements FhirPath {

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input,
      final @NotNull EvaluationContext context) {
    throw new NotImplementedException("Number literals are not yet implemented");
  }

}
