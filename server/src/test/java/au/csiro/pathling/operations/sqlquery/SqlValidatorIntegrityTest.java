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
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.annotation.Import;

/**
 * Build-time integrity checks for {@link SqlValidator}. Every entry in the allow-list / reject-list
 * must resolve to a class on the classpath (catches typos and Spark renames).
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

  @ParameterizedTest(name = "rejected expression: {0}")
  @MethodSource("rejectedExpressionFqns")
  @DisplayName("Every rejected expression FQN resolves to a class on the classpath")
  void rejectedExpressionFqnResolves(final String fqn) {
    assertThat(resolves(fqn)).as("FQN '%s' did not resolve on the classpath", fqn).isTrue();
  }

  static Stream<String> planNodeFqns() {
    return SqlValidator.allowedPlanNodes().stream();
  }

  static Stream<String> expressionFqns() {
    return SqlValidator.allowedExpressionNames().stream();
  }

  static Stream<String> rejectedExpressionFqns() {
    return SqlValidator.rejectedExpressionNames().stream();
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
