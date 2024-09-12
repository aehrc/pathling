package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;

public record ExternalConstant(String name) implements FhirPath {

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input,
      final @NotNull EvaluationContext context) {
    throw new NotImplementedException("External constants are not yet implemented");
  }

}
