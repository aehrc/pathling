package au.csiro.pathling.sql.boundary;

import java.math.BigDecimal;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Common functionality for boundary functions for decimals.
 */
public abstract class DecimalBoundaryFunction {

  private static final int MAX_PRECISION = 38;

  @Nullable
  protected static BigDecimal lowBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) throws Exception {
    if (d == null || !validatePrecision(d, precision)) {
      return null;
    }
    return new LowBoundaryForDecimal().call(d,
        Optional.ofNullable(precision).orElse(MAX_PRECISION));
  }

  @Nullable
  protected static BigDecimal highBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) throws Exception {
    if (d == null || !validatePrecision(d, precision)) {
      return null;
    }
    return new HighBoundaryForDecimal().call(d,
        Optional.ofNullable(precision).orElse(MAX_PRECISION));
  }

  @SuppressWarnings("BooleanMethodIsAlwaysInverted")
  private static boolean validatePrecision(@Nonnull final BigDecimal d,
      @Nullable final Integer precision) {
    if (precision == null) {
      return true;
    }
    final int integerLength = d.precision() - d.scale();
    final int maxScale = MAX_PRECISION - integerLength;
    return precision >= 0 && precision <= maxScale;
  }

}
