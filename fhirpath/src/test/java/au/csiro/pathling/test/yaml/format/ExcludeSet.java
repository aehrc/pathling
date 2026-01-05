/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

/** A collection of exclusion rules. */
@Data
@NoArgsConstructor
public class ExcludeSet {

  /** A title for the collection of exclusion rules. */
  @Nonnull String title;

  /** A longer description of what the exclusion set is concerned with. */
  @Nullable String comment;

  /** A glob which defines which test files this set covers. */
  @Nullable String glob;

  /** The list of exclusion rules in this set. */
  @Nonnull List<ExcludeRule> exclude;

  @Nonnull
  Stream<ExcludeRule> getExclusions(@Nonnull final Collection<String> disabledExclusionIds) {
    return exclude.stream()
        .filter(ex -> isNull(ex.getId()) || !disabledExclusionIds.contains(ex.id));
  }

  @Nonnull
  Stream<Predicate<TestCase>> toPredicates(@Nonnull final Collection<String> disabledExclusionIds) {
    return exclude.stream()
        .filter(ex -> isNull(ex.getId()) || !disabledExclusionIds.contains(ex.id))
        .flatMap(ex -> ex.toPredicates(title));
  }
}
