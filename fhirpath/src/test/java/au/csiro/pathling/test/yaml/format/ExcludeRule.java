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

import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ExcludeRule {

  public static final String OUTCOME_ERROR = "error";
  public static final String OUTCOME_FAILURE = "failure";
  public static final String OUTCOME_PASS = "pass";

  /** Provides a reference to a GitHub Issue. */
  @Nullable String id;

  /** Provides a descriptive name for the set of exclusions. */
  @Nonnull String title;

  /** Provides rationale on why this exclusion is in place. */
  @Nullable String comment;

  /** Provides a category for the exclusion, e.g. feature, bug. */
  @Nullable String type;

  /**
   * The expected outcome for the test if it is excluded. If this is not present in the YAML, or is
   * present and set to null, the outcome will be ERROR.
   */
  @Nonnull String outcome = OUTCOME_ERROR;

  /** Provides a way to disable the exclusion without removing it from the configuration file. */
  boolean disabled = false;

  /**
   * A list of function names to match within the expression to determine whether it should be
   * excluded.
   */
  @Nullable List<String> function;

  /**
   * A list of regular expressions that will be tested against the test expressions to determine
   * whether they should be excluded.
   */
  @Nullable List<String> expression;

  @Nullable List<String> desc;

  /** Expressions will be tested to see if they contain any of the strings in this list. */
  @Nullable List<String> any;

  /**
   * Test cases will be evaluated using these SpEL expressions to determine whether they should be
   * excluded.
   */
  @Nullable List<String> spel;

  @Nonnull
  @SuppressWarnings("unchecked")
  Stream<Predicate<TestCase>> toPredicates(@Nonnull final String category) {
    if (!disabled) {
      //noinspection RedundantCast
      return Stream.of(
              Stream.ofNullable(function).flatMap(List::stream).map(FunctionPredicate::of),
              Stream.ofNullable(expression).flatMap(List::stream).map(ExpressionPredicate::of),
              Stream.ofNullable(any).flatMap(List::stream).map(AnyPredicate::of),
              Stream.ofNullable(spel).flatMap(List::stream).map(SpELPredicate::of))
          .flatMap(Function.identity())
          .map(p -> TaggedPredicate.of((Predicate<TestCase>) p, title, category));
    } else {
      return Stream.empty();
    }
  }
}
