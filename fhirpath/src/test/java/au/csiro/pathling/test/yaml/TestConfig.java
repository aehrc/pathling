package au.csiro.pathling.test.yaml;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.yaml.snakeyaml.Yaml;

@Data
@NoArgsConstructor
public class TestConfig {

  private static final TestConfig DEFAULT = new TestConfig();

  private static final Yaml YAML_PARSER = new Yaml();

  @Value(staticConstructor = "of")
  static class FunctionPredicate implements Predicate<FhipathTestSpec.TestCase> {

    @Nonnull
    String function;

    @Override
    public boolean test(@Nonnull final FhipathTestSpec.TestCase testCase) {
      return testCase.getExpression().contains(function + "(");
    }
  }


  @Value(staticConstructor = "of")
  static class ExpressionPredicate implements Predicate<FhipathTestSpec.TestCase> {

    @Nonnull
    Pattern regex;

    @Override
    public boolean test(@Nonnull final FhipathTestSpec.TestCase testCase) {
      return regex.asPredicate().test(testCase.getExpression());
    }

    public static ExpressionPredicate of(@Nonnull final String expression) {
      return new ExpressionPredicate(Pattern.compile(expression));
    }
  }


  @Value(staticConstructor = "of")
  static class AnyPredicate implements Predicate<FhipathTestSpec.TestCase> {

    @Nonnull
    String substring;

    @Override
    public boolean test(@Nonnull final FhipathTestSpec.TestCase testCase) {
      return Stream.of(
          Stream.of(testCase.getExpression()),
          Stream.ofNullable(testCase.getDescription())
      ).flatMap(Function.identity()).anyMatch(s -> s.contains(substring));
    }
  }


  @Data
  @NoArgsConstructor
  public static class Exclude {

    @Nullable
    String comment;
    @Nullable
    String type;
    @Nullable
    List<String> function;
    @Nullable
    List<String> expression;
    @Nullable
    List<String> desc;
    @Nullable
    List<String> any;

    @Nonnull
    Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates() {
      return Stream.of(
          Stream.ofNullable(function).flatMap(List::stream)
              .map(FunctionPredicate::of),
          Stream.ofNullable(expression).flatMap(List::stream)
              .map(ExpressionPredicate::of),
          Stream.ofNullable(any).flatMap(List::stream)
              .map(AnyPredicate::of)
      ).flatMap(Function.identity());
    }
  }

  @Data
  @NoArgsConstructor
  public static class ExcludeSet {

    @Nullable
    String comment;
    String glob;
    @Nonnull
    List<Exclude> exclude;

    @Nonnull
    Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates() {
      return exclude.stream().flatMap(Exclude::toPredicates);
    }
  }


  @Nonnull
  List<ExcludeSet> excludeSet = List.of();

  @Nonnull
  Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates() {
    return excludeSet.stream().flatMap(ExcludeSet::toPredicates);
  }

  @Nonnull
  public Function<FhipathTestSpec.TestCase, Optional<String>> toExcluder() {
    final List<Predicate<FhipathTestSpec.TestCase>> predicates = toPredicates().toList();
    return testCase -> predicates
        .stream()
        .filter(p -> p.test(testCase))
        .findFirst()
        .map(Object::toString);
  }

  @Nonnull
  public static TestConfig fromYaml(@Nonnull final String yamlData) {
    return YAML_PARSER.loadAs(yamlData, TestConfig.class);
  }

  @Nonnull
  public static TestConfig getDefault() {
    return DEFAULT;
  }

}
