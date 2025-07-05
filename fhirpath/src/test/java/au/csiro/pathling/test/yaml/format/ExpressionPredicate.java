package au.csiro.pathling.test.yaml.format;

import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import jakarta.annotation.Nonnull;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import lombok.Value;

@Value(staticConstructor = "of")
class ExpressionPredicate implements Predicate<TestCase> {

  @Nonnull
  Pattern regex;

  @Override
  public boolean test(final TestCase testCase) {
    return regex.asPredicate().test(testCase.expression());
  }

  public static ExpressionPredicate of(@Nonnull final String expression) {
    return new ExpressionPredicate(Pattern.compile(expression));
  }
}
