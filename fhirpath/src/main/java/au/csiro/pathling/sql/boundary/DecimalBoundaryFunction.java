package au.csiro.pathling.sql.boundary;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Optional;
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
    final boolean precisionInvalid =
        precision != null && (precision <= 0 || precision > MAX_PRECISION);
    if (d == null || precisionInvalid) {
      return null;
    }

    final boolean inputIsNegative = d.compareTo(BigDecimal.ZERO) < 0;
    final boolean farBoundaryFromZero = isHigh ^ inputIsNegative;
    final int significantFigures = Optional.ofNullable(precision).orElse(MAX_PRECISION);

    // Round the number to the requested number of significant figures.
    final RoundingMode roundingMode = isHigh
                                      ? RoundingMode.CEILING
                                      : RoundingMode.FLOOR;
    final BigDecimal rounded = d.round(new MathContext(significantFigures, roundingMode));

    // Unscale the number to remove trailing zeroes.
    final BigDecimal stripped = rounded.stripTrailingZeros();
    final BigDecimal unscaled = new BigDecimal(stripped.unscaledValue());
    final boolean trailingZeroes = rounded.scale() > stripped.scale();

    // Expand the number to the far boundary if necessary.
    final BigDecimal expanded = farBoundaryFromZero && !trailingZeroes
                                ? inputIsNegative
                                  ? unscaled.subtract(almostHalf())
                                  : unscaled.add(almostHalf())
                                : unscaled;

    // Scale the number back to the original scale.
    final int power = stripped.scale();
    final RoundingMode divisionRoundingMode = inputIsNegative ^ power < 0
                                              ? RoundingMode.CEILING
                                              : RoundingMode.FLOOR;
    final BigDecimal result = expanded.divide(pow10(power, significantFigures),
        new MathContext(significantFigures, divisionRoundingMode));

    // Convert the result back into plain notation.
    return new BigDecimal(result.toPlainString());
  }

  private static BigDecimal almostHalf() {
    // Calculate the closest number to 1 that is less than 1.
    final BigDecimal half = new BigDecimal("0.5");
    final MathContext mc = new MathContext(MAX_PRECISION);
    return half.subtract(BigDecimal.valueOf(1, MAX_PRECISION), mc);
  }

  private static BigDecimal pow10(final int exponent, final int precision) {
    if (exponent == 0) {
      return BigDecimal.ONE; // Any number to the power of 0 is 1.
    }

    final int absExponent = Math.abs(exponent);
    final BigDecimal positivePower = BigDecimal.TEN.pow(absExponent);

    if (exponent > 0) {
      return positivePower;
    } else {
      // Calculate reciprocal for negative exponent.
      final MathContext mc = new MathContext(precision, RoundingMode.HALF_UP);
      return BigDecimal.ONE.divide(positivePower, mc);
    }
  }

}
