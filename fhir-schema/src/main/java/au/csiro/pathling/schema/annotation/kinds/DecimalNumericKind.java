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

package au.csiro.pathling.schema.annotation.kinds;

import static au.csiro.pathling.schema.LayoutFields.NUMERIC_ANNOTATION;

import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.AnnotationSwitch;
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * The numeric annotation of a decimal, {@code __numeric}: the value of the decimal's stored text,
 * at the engine's System.Decimal type (FR-002, decision 135).
 *
 * <p>It is the engine's own decoding of a stored decimal, so that reading the annotation gives
 * exactly what decoding the text gives (owner, 2026-10-26). The text stays the stored value, which
 * egress writes. The annotation is only the engine's fast path to the number.
 *
 * <p>The annotation is a field of the decimal's metadata group, {@code _<element>}, after its
 * {@code id} and {@code extension}.
 */
public final class DecimalNumericKind extends AnnotationKind {

  /** The kind. The set of kinds is closed, so there is only the one. */
  @Nonnull public static final DecimalNumericKind INSTANCE = new DecimalNumericKind();

  private DecimalNumericKind() {
    super(AnnotationSwitch.NUMERIC, List.of(NUMERIC_ANNOTATION));
  }

  /**
   * Computes the annotation of a decimal from its stored text.
   *
   * @param field the annotation, {@code __numeric}
   * @param value the stored text of the decimal
   * @return the annotation, typed as {@link SystemDecimal#TYPE}
   */
  @Override
  @Nonnull
  public Column compute(@Nonnull final String field, @Nonnull final Column value) {
    requireField(field);
    return decode(value);
  }

  /**
   * Takes a numeric annotation as the engine reads it: at the engine's System.Decimal type, decoded
   * from whatever type it was stored at, as {@link #decode} decodes any number.
   *
   * @param resolved the resolved annotation
   * @return the annotation, typed as {@link SystemDecimal#TYPE}
   */
  @Override
  @Nonnull
  public Column read(@Nonnull final Column resolved) {
    return decode(resolved);
  }

  /**
   * Decodes a number to the engine's System.Decimal type: the stored text of a decimal, a stored
   * numeric annotation of whatever type it was written at, or another number.
   *
   * <p>A value with more fractional digits than the type holds is rounded, and one beyond its range
   * is null. The cast is a safe one, so the result does not depend on the session's ANSI setting.
   *
   * @param number the stored text of a decimal, or a number
   * @return the value, typed as {@link SystemDecimal#TYPE}
   */
  @Nonnull
  public static Column decode(@Nonnull final Column number) {
    return number.try_cast(SystemDecimal.TYPE);
  }

  /**
   * Decodes a number known outside the plan to the value the engine's System.Decimal holds for it,
   * as {@link #decode} decodes it in the plan: rounded half up to the type's scale where it has
   * more fractional digits, and null where its integer digits are more than the type holds.
   *
   * <p>A value with no more fractional digits than the type holds keeps its own scale, so that the
   * value is unchanged where it does not need rounding.
   *
   * @param number the number, or null
   * @return the value the engine's System.Decimal holds for it, or null where there is no number or
   *     it does not fit
   */
  @Nullable
  public static BigDecimal decodeValue(@Nullable final BigDecimal number) {
    if (number == null) {
      return null;
    }
    final BigDecimal rounded =
        number.scale() > SystemDecimal.SCALE
            ? number.setScale(SystemDecimal.SCALE, RoundingMode.HALF_UP)
            : number;
    // Rounding may carry into a further integer digit, so the range is tested after it. Zero fits
    // whatever its exponent.
    return rounded.signum() != 0
            && rounded.precision() - rounded.scale() > SystemDecimal.INTEGER_DIGITS
        ? null
        : rounded;
  }
}
