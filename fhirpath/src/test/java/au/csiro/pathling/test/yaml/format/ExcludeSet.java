package au.csiro.pathling.test.yaml.format;

import static java.util.Objects.isNull;

import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A collection of exclusion rules.
 */
@Data
@NoArgsConstructor
public class ExcludeSet {

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
  List<ExcludeRule> exclude;

  @Nonnull
  Stream<ExcludeRule> getExclusions(
      @Nonnull final Collection<String> disabledExclusionIds) {
    return exclude.stream()
        .filter(ex -> isNull(ex.getId()) || !disabledExclusionIds.contains(ex.id));

  }

  @Nonnull
  Stream<Predicate<TestCase>> toPredicates(
      @Nonnull final Collection<String> disabledExclusionIds) {
    return exclude.stream()
        .filter(ex -> isNull(ex.getId()) || !disabledExclusionIds.contains(ex.id))
        .flatMap(ex -> ex.toPredicates(title));
  }
}
