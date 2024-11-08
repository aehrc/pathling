package au.csiro.pathling.fhirpath.expression;


import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.rendering.SingleColumnRendering;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import java.util.Optional;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.NotNull;

public record StringLiteral(FhirPathType type, String expression) implements FhirPath {

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input,
      final @NotNull EvaluationContext context) {
    return new Collection(new SingleColumnRendering(functions.lit(expression)),
        Optional.ofNullable(type));
  }

}
