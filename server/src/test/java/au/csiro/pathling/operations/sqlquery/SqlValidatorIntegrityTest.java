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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.test.SpringBootUnitTest;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.annotation.Import;

/**
 * Build-time integrity checks for {@link SqlValidator}. Every entry in the allow-list / reject-list
 * must resolve to a class on the classpath (catches typos and Spark renames), and every Spark
 * built-in function must be classified by {@link SqlFunctionAllowList}.
 */
@Import(SqlValidator.class)
@SpringBootUnitTest
class SqlValidatorIntegrityTest {

  @ParameterizedTest(name = "plan node: {0}")
  @MethodSource("planNodeFqns")
  @DisplayName("Every allowed plan node FQN resolves to a class on the classpath")
  void allowedPlanNodeFqnResolves(final String fqn) {
    assertThat(resolves(fqn)).as("FQN '%s' did not resolve on the classpath", fqn).isTrue();
  }

  @ParameterizedTest(name = "expression: {0}")
  @MethodSource("expressionFqns")
  @DisplayName("Every allowed expression FQN resolves to a class on the classpath")
  void allowedExpressionFqnResolves(final String fqn) {
    assertThat(resolves(fqn)).as("FQN '%s' did not resolve on the classpath", fqn).isTrue();
  }

  @ParameterizedTest(name = "aggregate function: {0}")
  @MethodSource("aggregateFunctionFqns")
  @DisplayName("Every allowed aggregate function FQN resolves to a class on the classpath")
  void allowedAggregateFunctionFqnResolves(final String fqn) {
    assertThat(resolves(fqn)).as("FQN '%s' did not resolve on the classpath", fqn).isTrue();
  }

  @ParameterizedTest(name = "rejected expression: {0}")
  @MethodSource("rejectedExpressionFqns")
  @DisplayName("Every rejected expression FQN resolves to a class on the classpath")
  void rejectedExpressionFqnResolves(final String fqn) {
    assertThat(resolves(fqn)).as("FQN '%s' did not resolve on the classpath", fqn).isTrue();
  }

  @ParameterizedTest(name = "describe command: {0}")
  @MethodSource("describeCommandFqns")
  @DisplayName("Every DESCRIBE carve-out FQN resolves to a class on the classpath")
  void describeCommandFqnResolves(final String fqn) {
    assertThat(resolves(fqn)).as("FQN '%s' did not resolve on the classpath", fqn).isTrue();
  }

  // A Spark upgrade that adds a built-in function fails this test until the function has been
  // vetted and placed on one list or the other. A function that has been removed or renamed fails
  // it too, so that neither list carries stale entries.
  @Test
  @DisplayName("Every Spark built-in function is classified as either allowed or rejected")
  void everyBuiltInFunctionIsClassified() {
    final Set<String> classified = new HashSet<>(SqlFunctionAllowList.ALLOWED_FUNCTION_NAMES);
    classified.addAll(SqlFunctionAllowList.REJECTED_FUNCTION_NAMES);

    assertThat(classified)
        .as("built-in functions not classified, or classified names that are not built-ins")
        .containsExactlyInAnyOrderElementsOf(builtInFunctionNames());
  }

  @Test
  @DisplayName("No function is both allowed and rejected")
  void noFunctionIsBothAllowedAndRejected() {
    assertThat(SqlFunctionAllowList.ALLOWED_FUNCTION_NAMES)
        .doesNotContainAnyElementsOf(SqlFunctionAllowList.REJECTED_FUNCTION_NAMES);
  }

  static Stream<String> planNodeFqns() {
    return SqlValidator.allowedPlanNodes().stream();
  }

  static Stream<String> expressionFqns() {
    return SqlValidator.allowedExpressionNames().stream();
  }

  static Stream<String> aggregateFunctionFqns() {
    return SqlValidator.allowedAggregateFunctionNames().stream();
  }

  static Stream<String> rejectedExpressionFqns() {
    return SqlValidator.rejectedExpressionNames().stream();
  }

  static Stream<String> describeCommandFqns() {
    return SqlValidator.describeCommandNames().stream();
  }

  /** Returns the names of all functions in Spark's built-in function registry. */
  private static Set<String> builtInFunctionNames() {
    return scala.jdk.javaapi.CollectionConverters.asJava(FunctionRegistry.builtin().listFunction())
        .stream()
        .map(FunctionIdentifier::funcName)
        .collect(Collectors.toSet());
  }

  /**
   * Tries to load the class by FQN; falls back to the {@code $}-suffixed form for Scala case
   * objects.
   */
  private static boolean resolves(final String fqn) {
    try {
      Class.forName(fqn);
      return true;
    } catch (final ClassNotFoundException ignored) {
      try {
        Class.forName(fqn + "$");
        return true;
      } catch (final ClassNotFoundException ignored2) {
        return false;
      }
    }
  }
}
