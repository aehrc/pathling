/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  @Nonnull List<ExcludeSet> excludeSet = List.of();

  @Nonnull
  public Stream<Predicate<TestCase>> toPredicates(
      @Nonnull final Collection<String> disabledExclusionIds) {
    return excludeSet.stream().flatMap(es -> es.toPredicates(disabledExclusionIds));
  }

  @Nonnull
  public Function<TestCase, Optional<ExcludeRule>> toExcluder(
      @Nonnull final Collection<String> disabledExclusionIds) {
    final List<ExcludeRule> exclusions =
        excludeSet.stream().flatMap(es -> es.getExclusions(disabledExclusionIds)).toList();
    return testCase ->
        exclusions.stream()
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
