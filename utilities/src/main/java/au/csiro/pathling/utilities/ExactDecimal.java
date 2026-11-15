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

package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * A decimal held exactly, as its unscaled value of up to 38 digits and its scale, defined once for
 * the engine's flexible decimal and for the exact canonical value of a quantity, which the engine
 * reads back as one, so that the two cannot disagree on its type or on how a value is fitted to it.
 */
public final class ExactDecimal {

  /** The maximum precision (the number of significant digits). */
  public static final int MAX_PRECISION = 38;

  /** The name of the field holding the unscaled value. */
  @Nonnull public static final String VALUE_FIELD = "value";

  /** The name of the field holding the scale. */
  @Nonnull public static final String SCALE_FIELD = "scale";

  /** The type: the unscaled value, as {@code DECIMAL(38,0)}, and the scale. */
  @Nonnull
  public static final StructType TYPE =
      new StructType()
          .add(VALUE_FIELD, DataTypes.createDecimalType(MAX_PRECISION, 0))
          .add(SCALE_FIELD, DataTypes.IntegerType);

  private ExactDecimal() {}

  /**
   * Fits a decimal to the maximum precision, with a scale that is never negative: fractional digits
   * beyond the precision are rounded half up, and a value whose integer digits alone are too many
   * has no exact form.
   *
   * @param decimal the decimal to fit
   * @return the fitted decimal, or null where there is none or it cannot be represented
   */
  @Nullable
  public static BigDecimal normalize(@Nullable final BigDecimal decimal) {
    if (decimal == null) {
      return null;
    }
    final BigDecimal integral = decimal.scale() < 0 ? decimal.setScale(0) : decimal;
    if (integral.precision() <= MAX_PRECISION) {
      return integral;
    }
    // Rounding to a precision rather than to a scale accounts for a carry, which adds a digit, as
    // 0.99…9 does in rounding to 1.00…0.
    final BigDecimal rounded = integral.round(new MathContext(MAX_PRECISION, RoundingMode.HALF_UP));
    return rounded.scale() < 0 ? null : rounded;
  }
}
