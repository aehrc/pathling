package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

public record Traversal(Optional<FhirPath> source, String target) implements FhirPath {

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input, final @NotNull EvaluationContext context) {
    return input.traverse(target);
  }

}
