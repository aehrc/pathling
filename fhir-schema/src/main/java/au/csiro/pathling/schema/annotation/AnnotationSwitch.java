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

package au.csiro.pathling.schema.annotation;

import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.util.function.Predicate;

/**
 * The switch of {@link SchemaConfiguration} that governs whether an annotation kind is stored.
 *
 * <p>The set of switches is closed, as the set of kinds is: one for the numeric annotation of a
 * decimal, one for the range annotations of a date, and one for the canonical annotation of a
 * quantity. A kind carried in more than one field is governed by one switch.
 */
public final class AnnotationSwitch {

  /** The switch governing the numeric annotation of a decimal. */
  @Nonnull
  public static final AnnotationSwitch NUMERIC =
      new AnnotationSwitch("numeric", SchemaConfiguration::isEnableNumericAnnotation);

  /** The switch governing the range annotations of a date, dateTime or instant. */
  @Nonnull
  public static final AnnotationSwitch RANGE =
      new AnnotationSwitch("range", SchemaConfiguration::isEnableRangeAnnotation);

  /** The switch governing the canonical annotation of a quantity. */
  @Nonnull
  public static final AnnotationSwitch CANONICAL =
      new AnnotationSwitch("canonical", SchemaConfiguration::isEnableCanonicalAnnotation);

  @Nonnull private final String name;

  @Nonnull private final Predicate<SchemaConfiguration> enabled;

  private AnnotationSwitch(
      @Nonnull final String name, @Nonnull final Predicate<SchemaConfiguration> enabled) {
    this.name = name;
    this.enabled = enabled;
  }

  /**
   * Returns whether this switch is on in a configuration.
   *
   * @param configuration the configuration to consult
   * @return true where the kinds this switch governs are stored
   */
  public boolean isOn(@Nonnull final SchemaConfiguration configuration) {
    return enabled.test(configuration);
  }

  @Override
  @Nonnull
  public String toString() {
    return name;
  }
}
