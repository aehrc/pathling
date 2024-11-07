package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

public record Traversal(Optional<FhirPath> source, String target) implements FhirPath {

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input, final @NotNull EvaluationContext context) {
    // If we are at the root context and the target is the same as the input type, this is just a
    // reference to the input type itself and is a no-op.
    if (source.isEmpty() && 
        input.getType().isPresent() && 
        input.getType().get().getTypeName().equals(target)) {
      return input;
    }
    return input.traverse(target);
  }

}
