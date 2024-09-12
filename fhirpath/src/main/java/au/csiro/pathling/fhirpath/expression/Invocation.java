package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import org.jetbrains.annotations.NotNull;

public record Invocation(FhirPath source, FhirPath target) implements FhirPath {

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input,
      final @NotNull EvaluationContext context) {
    final Collection sourceResult = source.evaluate(input, context);
    return target.evaluate(sourceResult, context);
  }

}
