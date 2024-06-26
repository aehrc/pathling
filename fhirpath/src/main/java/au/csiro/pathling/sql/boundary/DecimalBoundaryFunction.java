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
  private static final String NINES = "99999999999999999999999999999999999999";

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
      @Nullable final Integer precision, boolean isHigh) {
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

    BigDecimal result = d;
    if (isHigh) {
      // Add the maximum number of nines to the decimal.
      result = new BigDecimal(d.toPlainString() + (d.scale() == 0
                                                   ? "."
                                                   : "") + NINES);
    }

    // Round the result to the desired precision.
    return result.setScale(Objects.requireNonNullElse(precision, maxScale), RoundingMode.DOWN);
  }

}
