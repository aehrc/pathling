package au.csiro.pathling.test.yaml.format;

import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Predicate;
import lombok.Value;

@Value(staticConstructor = "of")
class FunctionPredicate implements Predicate<TestCase> {

  @Nonnull
  String function;

  @Override
  public boolean test(final TestCase testCase) {
    Objects.requireNonNull(testCase);
    return testCase.expression().contains(function + "(");
  }
}
