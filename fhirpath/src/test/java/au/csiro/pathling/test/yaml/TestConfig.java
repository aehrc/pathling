package au.csiro.pathling.test.yaml;

import static java.util.Objects.isNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.yaml.snakeyaml.Yaml;

/**
 * Represents configuration that relates to how the YAML-based test cases are executed.
 *
 * @author Piotr Szul
 */
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
    public boolean test(final FhipathTestSpec.TestCase testCase) {
      Objects.requireNonNull(testCase);
      return testCase.getExpression().contains(function + "(");
    }
  }


  @Value(staticConstructor = "of")
  static class ExpressionPredicate implements Predicate<FhipathTestSpec.TestCase> {

    @Nonnull
    Pattern regex;

    @Override
    public boolean test(final FhipathTestSpec.TestCase testCase) {
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
    public boolean test(final FhipathTestSpec.TestCase testCase) {
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
    public boolean test(final FhipathTestSpec.TestCase testCase) {
      final EvaluationContext context = new StandardEvaluationContext();
      context.setVariable("testCase", testCase);
      final Expression exp = PARSER.parseExpression(spELExpression);
      return Boolean.TRUE.equals(exp.getValue(context, Boolean.class));
    }
  }

  @Value(staticConstructor = "of")
  static class TaggedPredicate implements Predicate<FhipathTestSpec.TestCase> {

    @Nonnull
    Predicate<FhipathTestSpec.TestCase> predicate;

    @Nonnull
    String tag;

    @Override
    public boolean test(final FhipathTestSpec.TestCase testCase) {
      return predicate.test(testCase);
    }

    @Override
    @Nonnull
    public String toString() {
      return predicate + "#" + tag;
    }

    @Nonnull
    static TaggedPredicate of(@Nonnull final Predicate<FhipathTestSpec.TestCase> predicate,
        @Nonnull final String title, @Nonnull final String category) {
      try {
        // Create a MessageDigest instance for MD5
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        // Update the digest with the bytes of the data
        final String data = category + title;
        final byte[] hashBytes = digest.digest(data.getBytes(StandardCharsets.UTF_8));
        // Convert the hash bytes to a hexadecimal string
        return TaggedPredicate.of(predicate,
            DatatypeConverter.printHexBinary(hashBytes).toLowerCase());
      } catch (final NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  }


  @Data
  @NoArgsConstructor
  public static class Exclude {

    /**
     * Provides a reference to a GitHub Issue.
     */
    @Nullable
    String id;

    /**
     * Provides a descriptive name for the set of exclusions.
     */
    @Nonnull
    String title;

    /**
     * Provides rationale on why this exclusion is in place.
     */
    @Nullable
    String comment;

    /**
     * Provides a category for the exclusion, e.g. feature, bug.
     */
    @Nullable
    String type;

    /**
     * Provides a way to disable the exclusion without removing it from the configuration file.
     */
    boolean disabled = false;

    /**
     * A list of function names to match within the expression to determine whether it should be
     * excluded.
     */
    @Nullable
    List<String> function;

    /**
     * A list of regular expressions that will be tested against the test expressions to determine
     * whether they should be excluded.
     */
    @Nullable
    List<String> expression;

    @Nullable
    List<String> desc;

    /**
     * Expressions will be tested to see if they contain any of the strings in this list.
     */
    @Nullable
    List<String> any;

    /**
     * Test cases will be evaluated using these SpEL expressions to determine whether they should be
     * excluded.
     */
    @Nonnull
    List<String> spel;

    @Nonnull
    Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates(@Nonnull final String category) {
      if (!disabled) {
        //noinspection RedundantCast
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
                p -> TaggedPredicate.of((Predicate<FhipathTestSpec.TestCase>) p, title, category));
      } else {
        return Stream.empty();
      }
    }
  }

  /**
   * A collection of exclusion rules.
   */
  @Data
  @NoArgsConstructor
  static class ExcludeSet {

    /**
     * A title for the collection of exclusion rules.
     */
    @Nonnull
    String title;

    /**
     * A longer description of what the exclusion set is concerned with.
     */
    @Nullable
    String comment;

    /**
     * A glob which defines which test files this set covers.
     */
    @Nullable
    String glob;

    /**
     * The list of exclusion rules in this set.
     */
    @Nonnull
    List<Exclude> exclude;

    @Nonnull
    private Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates(
        @Nonnull final Collection<String> disabledExclusionIds) {
      return exclude.stream()
          .filter(ex -> isNull(ex.getId()) || !disabledExclusionIds.contains(ex.id))
          .flatMap(ex -> ex.toPredicates(title));
    }
  }

  @Nonnull
  List<ExcludeSet> excludeSet = List.of();

  @Nonnull
  Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates(
      @Nonnull final Collection<String> disabledExclusionIds) {
    return excludeSet.stream().flatMap(es -> es.toPredicates(disabledExclusionIds));
  }


  @Nonnull
  public Function<FhipathTestSpec.TestCase, Optional<String>> toExcluder(
      @Nonnull final Collection<String> disabledExclusionIds) {
    final List<Predicate<FhipathTestSpec.TestCase>> predicates = toPredicates(
        disabledExclusionIds).toList();
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
