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
package au.csiro.pathling.schema;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.Test;

/**
 * Tests that the options this module's configuration carries are readable and arrive with their
 * documented defaults.
 *
 * <p>This is the configuration surface itself rather than its effect. Mode parity is asserted by
 * {@link SchemaModeParityTest} against the derivation, and the effect of the annotation toggles is
 * asserted against the transform that emits them; neither reaches the defaults, and a default that
 * silently changed would change what every caller that states nothing gets.
 *
 * <p>Each toggle is asserted to be independent of the others, because "individually disableable"
 * (FR-021) is exactly the property that two options sharing one field would break, while a test
 * that only ever turned one off at a time would still pass.
 */
class SchemaConfigurationTest {

  @Nonnull
  private static SchemaConfiguration defaults() {
    return SchemaConfiguration.builder().build();
  }

  /**
   * The schema is fitted to the data unless the caller asks otherwise, so that the dense mode is
   * the option and not the baseline (FR-009).
   */
  @Test
  void prunesTheSchemaByDefault() {
    assertFalse(defaults().isDenseSchema());
  }

  /** Content the definitions do not describe is ignored unless the caller asks for a failure. */
  @Test
  void ignoresNonConformantContentByDefault() {
    assertFalse(defaults().isFailOnNonConformantContent());
  }

  /** Every annotation is emitted unless the caller disables it (FR-021). */
  @Test
  void emitsEveryAnnotationByDefault() {
    final SchemaConfiguration configuration = defaults();
    assertTrue(configuration.isEnableNumericAnnotation());
    assertTrue(configuration.isEnableRangeAnnotation());
    assertTrue(configuration.isEnableCanonicalAnnotation());
  }

  /** The dense mode is readable back as it was set. */
  @Test
  void carriesTheSchemaModeItWasGiven() {
    assertTrue(SchemaConfiguration.builder().denseSchema(true).build().isDenseSchema());
    assertFalse(SchemaConfiguration.builder().denseSchema(false).build().isDenseSchema());
  }

  /** The strictness switch is readable back as it was set. */
  @Test
  void carriesTheStrictnessSwitchItWasGiven() {
    assertTrue(
        SchemaConfiguration.builder()
            .failOnNonConformantContent(true)
            .build()
            .isFailOnNonConformantContent());
    assertFalse(
        SchemaConfiguration.builder()
            .failOnNonConformantContent(false)
            .build()
            .isFailOnNonConformantContent());
  }

  /** Disabling the numeric annotation leaves the other two emitted. */
  @Test
  void disablesTheNumericAnnotationAlone() {
    final SchemaConfiguration configuration =
        SchemaConfiguration.builder().enableNumericAnnotation(false).build();
    assertFalse(configuration.isEnableNumericAnnotation());
    assertTrue(configuration.isEnableRangeAnnotation());
    assertTrue(configuration.isEnableCanonicalAnnotation());
  }

  /** Disabling the range annotation leaves the other two emitted. */
  @Test
  void disablesTheRangeAnnotationAlone() {
    final SchemaConfiguration configuration =
        SchemaConfiguration.builder().enableRangeAnnotation(false).build();
    assertTrue(configuration.isEnableNumericAnnotation());
    assertFalse(configuration.isEnableRangeAnnotation());
    assertTrue(configuration.isEnableCanonicalAnnotation());
  }

  /** Disabling the canonical annotation leaves the other two emitted. */
  @Test
  void disablesTheCanonicalAnnotationAlone() {
    final SchemaConfiguration configuration =
        SchemaConfiguration.builder().enableCanonicalAnnotation(false).build();
    assertTrue(configuration.isEnableNumericAnnotation());
    assertTrue(configuration.isEnableRangeAnnotation());
    assertFalse(configuration.isEnableCanonicalAnnotation());
  }

  /** All three annotations can be disabled at once, which is the annotation-free run (FR-022). */
  @Test
  void disablesEveryAnnotationTogether() {
    final SchemaConfiguration configuration =
        SchemaConfiguration.builder()
            .enableNumericAnnotation(false)
            .enableRangeAnnotation(false)
            .enableCanonicalAnnotation(false)
            .build();
    assertFalse(configuration.isEnableNumericAnnotation());
    assertFalse(configuration.isEnableRangeAnnotation());
    assertFalse(configuration.isEnableCanonicalAnnotation());
  }
}
