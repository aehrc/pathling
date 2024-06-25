package au.csiro.pathling.sql.boundary;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.function.BinaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Common functionality for boundary functions for decimals.
 *
 * @author John Grimes
 */
public abstract class DecimalBoundaryFunction {

  private static final int MAX_PRECISION = 37;

  @Nullable
  protected static BigDecimal lowBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) {
    if (d == null) {
      return null;
    }
    return calculateBoundary(d, precision, BigDecimal::subtract);
  }

  @Nullable
  protected static BigDecimal highBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) {
    if (d == null) {
      return null;
    }
    return calculateBoundary(d, precision, BigDecimal::add);
  }

  @Nullable
  private static BigDecimal calculateBoundary(@Nonnull final BigDecimal input,
      @Nullable final Integer precision, @Nonnull final BinaryOperator<BigDecimal> operator) {
    final int maxPrecision = MAX_PRECISION - (input.precision() - input.scale());
    final int resolvedPrecision = Objects.requireNonNullElse(precision, maxPrecision);
    if (resolvedPrecision < 0 || resolvedPrecision > maxPrecision) {
      // If the requested precision is negative or greater than the maximum, return an empty result.
      return null;
    }
    final BigDecimal unit = BigDecimal.ONE.scaleByPowerOfTen(-resolvedPrecision);
    final BigDecimal halfUnit = unit.divide(BigDecimal.valueOf(2), unit.scale() + 1,
        RoundingMode.HALF_UP);
    return operator.apply(input, halfUnit);
  }

}
