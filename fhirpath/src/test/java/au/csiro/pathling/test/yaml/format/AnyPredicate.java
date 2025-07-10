package au.csiro.pathling.test.yaml.format;

import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import jakarta.annotation.Nonnull;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.Value;

@Value(staticConstructor = "of")
class AnyPredicate implements Predicate<TestCase> {

  @Nonnull
  String substring;

  @Override
  public boolean test(final TestCase testCase) {
    return Stream.of(
        Stream.of(testCase.expression()),
        Stream.ofNullable(testCase.description())
    ).flatMap(Function.identity()).anyMatch(s -> s.contains(substring));
  }
}
