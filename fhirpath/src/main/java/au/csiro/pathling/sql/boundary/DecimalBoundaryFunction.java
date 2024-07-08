package au.csiro.pathling.sql.boundary;

import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Optional;

/**
 * Common functionality for boundary functions for decimals.
 */
public abstract class DecimalBoundaryFunction {

  private static final int MAX_PRECISION = 38;

  @Nullable
  protected static BigDecimal lowBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) throws Exception {
    final Optional<Integer> maxScale = getMaxScale(d, precision);
    if (maxScale.isEmpty()) {
      return null;
    }
    return new LowBoundaryForDecimal().call(d,
        Optional.ofNullable(precision).orElse(MAX_PRECISION));
  }

  @Nullable
  protected static BigDecimal highBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) throws Exception {
    final Optional<Integer> maxScale = getMaxScale(d, precision);
    if (maxScale.isEmpty()) {
      return null;
    }
    return new HighBoundaryForDecimal().call(d,
        Optional.ofNullable(precision).orElse(MAX_PRECISION));
  }

  private static Optional<Integer> getMaxScale(@Nullable final BigDecimal d,
      @Nullable final Integer precision) {
    if (d == null) {
      return Optional.empty();
    }
    final int integerLength = d.precision() - d.scale();
    final int maxScale = MAX_PRECISION - integerLength;
    if (precision == null) {
      return Optional.of(maxScale);
    }
    return precision >= 0 && precision <= maxScale
           ? Optional.of(maxScale)
           : Optional.empty();
  }

}
