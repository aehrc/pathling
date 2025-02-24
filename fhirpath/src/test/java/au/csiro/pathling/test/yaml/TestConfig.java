package au.csiro.pathling.test.yaml;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
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

  @Value(staticConstructor = "of")
  static class SpELPredicate implements Predicate<FhipathTestSpec.TestCase> {

    private static final ExpressionParser PARSER = new SpelExpressionParser();

    @Nonnull
    String spELExpression;

    @Override
    public boolean test(@Nonnull final FhipathTestSpec.TestCase testCase) {
      StandardEvaluationContext context = new StandardEvaluationContext();
      context.setVariable("testCase", testCase);
      Expression exp = PARSER.parseExpression(spELExpression);
      return Boolean.TRUE.equals(exp.getValue(context, Boolean.class));
    }
  }

  @Value(staticConstructor = "of")
  static class TagggedPredicate implements Predicate<FhipathTestSpec.TestCase> {

    @Nonnull
    Predicate<FhipathTestSpec.TestCase> predicate;

    @Nonnull
    String tag;

    @Override
    public boolean test(@Nonnull final FhipathTestSpec.TestCase testCase) {
      return predicate.test(testCase);
    }

    @Override
    @Nonnull
    public String toString() {
      return predicate + "#" + tag;
    }

    @Nonnull
    static TagggedPredicate of(@Nonnull final Predicate<FhipathTestSpec.TestCase> predicate,
        @Nonnull final String title, @Nonnull final String category) {
      try {
        // Create a MessageDigest instance for MD5
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        // Update the digest with the bytes of the data
        final String data = category + title;
        byte[] hashBytes = digest.digest(data.getBytes(StandardCharsets.UTF_8));
        // Convert the hash bytes to a hexadecimal string
        return TagggedPredicate.of(predicate,
            DatatypeConverter.printHexBinary(hashBytes).toLowerCase());
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  }


  @Data
  @NoArgsConstructor
  public static class Exclude {

    @Nullable
    String id;
    @Nullable
    String title;
    @Nullable
    String comment;
    @Nullable
    String type;
    @Nullable
    boolean disabled = false;
    List<String> function;
    @Nullable
    List<String> expression;
    @Nullable
    List<String> desc;
    @Nullable
    List<String> any;
    @Nonnull
    List<String> spel;

    @Nonnull
    Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates(@Nonnull final String category) {
      if (!disabled) {
        return Stream.of(
                Stream.ofNullable(function).flatMap(List::stream)
                    .map(FunctionPredicate::of),
                Stream.ofNullable(expression).flatMap(List::stream)
                    .map(ExpressionPredicate::of),
                Stream.ofNullable(any).flatMap(List::stream)
                    .map(AnyPredicate::of),
                Stream.ofNullable(spel).flatMap(List::stream)
                    .map(SpELPredicate::of)
            ).flatMap(Function.identity())
            .map(
                p -> TagggedPredicate.of((Predicate<FhipathTestSpec.TestCase>) p, title, category));
      } else {
        return Stream.empty();
      }
    }
  }

  @Data
  @NoArgsConstructor
  public static class ExcludeSet {

    @Nullable
    String title;
    @Nullable
    String comment;
    @Nullable
    String glob;
    @Nonnull
    List<Exclude> exclude;

    @Nonnull
    Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates() {
      return exclude.stream().flatMap(ex -> ex.toPredicates(title));
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
