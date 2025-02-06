package au.csiro.pathling.test.yaml;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

@Data
@NoArgsConstructor
public class TestConfig {

  private static final Yaml YAML_PARSER = new Yaml();

  @Value(staticConstructor = "of")
  static class FunctionPredicate implements Predicate<FhipathTestSpec.TestCase> {

    @Nonnull
    String function;
    
    @Override
    public boolean test(@Nonnull final FhipathTestSpec.TestCase testCase) {
      return testCase.getExpression().contains(function);
    }
  }


  @Data
  @NoArgsConstructor
  public static class Exclude {

    @Nullable
    String comment;
    @Nullable
    List<String> function;
    @Nullable
    List<String> expression;
    @Nullable
    List<String> desc;

    @Nonnull
    Stream<Predicate<FhipathTestSpec.TestCase>> toPredicates() {
      return Optional.ofNullable(function).stream().flatMap(List::stream)
          .map(FunctionPredicate::of);
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
  List<ExcludeSet> excludeSet;

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

}
