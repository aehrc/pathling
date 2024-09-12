package au.csiro.pathling.fhirpath.expression;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.function.FunctionInput;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public record FunctionCall(String name, List<FhirPath> arguments) implements FhirPath {

  @Override
  public @NotNull Collection evaluate(final @NotNull Collection input,
      final @NotNull EvaluationContext context) {
    final NamedFunction<? extends Collection> function = context.functionRegistry()
        .getInstance(name)
        .orElseThrow(() -> new IllegalArgumentException("Unknown function: " + name));
    final FunctionInput functionInput = new FunctionInput(context, input, arguments);
    return function.invoke(functionInput);
  }

}
