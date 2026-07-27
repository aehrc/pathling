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

package au.csiro.pathling.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryConfiguration}, covering the {@code maxDependencyDepth} default and
 * its {@code @Min(1)} validation, and guarding against the reintroduction of a server-side row cap
 * or query timeout.
 *
 * @author John Grimes
 */
class SqlQueryConfigurationTest {

  private Validator validator;

  @BeforeEach
  void setUp() {
    validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  @Test
  void exposesOnlyTheDependencyDepthSetting() {
    // The dependency-depth limit is the only resource limit this configuration carries. A row cap
    // silently truncates results and a wall-clock timeout aborts legitimate long-running work, so
    // neither may return: this test fails if a new setting is added here.
    final Set<String> declaredSettings =
        Arrays.stream(SqlQueryConfiguration.class.getDeclaredFields())
            .filter(field -> !field.isSynthetic())
            .filter(field -> !Modifier.isStatic(field.getModifiers()))
            .map(Field::getName)
            .collect(java.util.stream.Collectors.toSet());

    assertThat(declaredSettings).containsExactly("maxDependencyDepth");
  }

  @Test
  void defaultMaxDependencyDepthIsTen() {
    // The default must be a generous-but-bounded value so that real, shallow view graphs are never
    // rejected while pathological fan-out is still capped.
    assertThat(new SqlQueryConfiguration().getMaxDependencyDepth()).isEqualTo(10);
  }

  @Test
  void acceptsPositiveMaxDependencyDepth() {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setMaxDependencyDepth(1);

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).isEmpty();
  }

  @Test
  void rejectsZeroMaxDependencyDepth() {
    // A depth of zero would forbid even a single dependency, which is nonsensical for a feature
    // whose purpose is dependency resolution.
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setMaxDependencyDepth(0);

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).isNotEmpty();
  }

  @Test
  void rejectsNegativeMaxDependencyDepth() {
    final SqlQueryConfiguration config = new SqlQueryConfiguration();
    config.setMaxDependencyDepth(-5);

    final Set<ConstraintViolation<SqlQueryConfiguration>> violations = validator.validate(config);

    assertThat(violations).isNotEmpty();
  }
}
