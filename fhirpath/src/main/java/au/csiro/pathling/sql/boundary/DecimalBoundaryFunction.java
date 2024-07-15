package au.csiro.pathling.sql.boundary;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Common functionality for boundary functions for decimals.
 */
public abstract class DecimalBoundaryFunction {

  private static final int MAX_PRECISION = 38;

  @Nullable
  protected static BigDecimal lowBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) {
    return calculateBoundary(d, precision, false);
  }

  @Nullable
  protected static BigDecimal highBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) {
    return calculateBoundary(d, precision, true);
  }

  @Nullable
  private static BigDecimal calculateBoundary(@Nullable final BigDecimal d,
      @Nullable final Integer precision, final boolean isHigh) {
    // Check for null or invalid precision.
    if (d == null || (precision != null && precision < 0)) {
      return null;
    }

    // Calculate the maximum scale that will fit within the maximum decimal.
    final int integerLength = d.precision() - d.scale();
    final int maxScale = MAX_PRECISION - integerLength;
    if (precision != null && precision > maxScale) {
      return null;
    }

    // Determine the digits that will potentially need to be added to the number based on whether
    // it is negative and whether we are calculating a boundary that is further or closer to zero.
    final boolean inputIsNegative = d.compareTo(BigDecimal.ZERO) < 0;
    final boolean farBoundaryFromZero = isHigh ^ inputIsNegative;
    final String digit = farBoundaryFromZero
                         ? "9"
                         : "0";

    BigDecimal result = d;
    if (farBoundaryFromZero) {
      // Add the necessary number of extra digits to the decimal.
      final int additionalDigits = Objects.requireNonNullElse(precision, maxScale) - d.scale();
      if (additionalDigits > 0) {
        result = new BigDecimal(d.toPlainString() + (d.scale() == 0
                                                     ? "."
                                                     : "") + digit.repeat(additionalDigits));
      }
    }

    // Determine the correct rounding mode based on whether we are calculating a high or low
    // boundary.
    final RoundingMode roundingMode = isHigh
                                      ? RoundingMode.CEILING
                                      : RoundingMode.FLOOR;
    // Round the result to the desired precision.
    return result.setScale(Objects.requireNonNullElse(precision, maxScale), roundingMode);
  }

}
