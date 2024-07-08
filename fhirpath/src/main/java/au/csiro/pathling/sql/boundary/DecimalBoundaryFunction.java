package au.csiro.pathling.sql.boundary;

import java.math.BigDecimal;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Common functionality for boundary functions for decimals.
 */
public abstract class DecimalBoundaryFunction {

  private static final int MAX_PRECISION = 38;

  @Nullable
  protected static BigDecimal lowBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) throws Exception {
    return new LowBoundaryForDecimal().call(d,
        Optional.ofNullable(precision).orElse(MAX_PRECISION));
  }

  @Nullable
  protected static BigDecimal highBoundaryForDecimal(@Nullable final BigDecimal d,
      @Nullable final Integer precision) throws Exception {
    return new HighBoundaryForDecimal().call(d,
        Optional.ofNullable(precision).orElse(MAX_PRECISION));
  }

}
