package au.csiro.pathling.fhirpath.expression;


import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import org.jetbrains.annotations.NotNull;

public record StringLiteral(FhirPathType type, String expression) implements FhirPath {

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input,
      final @NotNull EvaluationContext context) {
    return StringCollection.fromLiteral(expression);
  }

}
