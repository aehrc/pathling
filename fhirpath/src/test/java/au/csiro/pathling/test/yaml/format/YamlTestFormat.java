package au.csiro.pathling.test.yaml.format;

import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.yaml.snakeyaml.Yaml;

/**
 * Represents configuration that relates to how the YAML-based test cases are executed.
 *
 * @author Piotr Szul
 */
@Data
@NoArgsConstructor
public class YamlTestFormat {

  private static final YamlTestFormat DEFAULT = new YamlTestFormat();
  private static final Yaml YAML_PARSER = new Yaml();

  @Nonnull
  List<ExcludeSet> excludeSet = List.of();

  @Nonnull
  public Stream<Predicate<TestCase>> toPredicates(
      @Nonnull final Collection<String> disabledExclusionIds) {
    return excludeSet.stream().flatMap(es -> es.toPredicates(disabledExclusionIds));
  }


  @Nonnull
  public Function<TestCase, Optional<ExcludeRule>> toExcluder(
      @Nonnull final Collection<String> disabledExclusionIds) {
    final List<ExcludeRule> exclusions = excludeSet.stream()
        .flatMap(es -> es.getExclusions(disabledExclusionIds)).toList();
    return testCase -> exclusions.stream()
        .filter(ex -> ex.toPredicates(ex.getTitle()).anyMatch(p -> p.test(testCase)))
        .findFirst();
  }

  @Nonnull
  public static YamlTestFormat fromYaml(@Nonnull final String yamlData) {
    return YAML_PARSER.loadAs(yamlData, YamlTestFormat.class);
  }

  @Nonnull
  public static YamlTestFormat getDefault() {
    return DEFAULT;
  }

}
