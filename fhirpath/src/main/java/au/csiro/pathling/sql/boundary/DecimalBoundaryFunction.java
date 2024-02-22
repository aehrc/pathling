package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Common functionality for boundary functions for decimals.
 *
 * @author John Grimes
 */
public abstract class DecimalBoundaryFunction {

  /**
   * Get the boundary for a decimal, given an original scale and a function to calculate the
   * boundary.
   *
   * @param d the decimal
   * @param scale the original scale
   * @param function the function to calculate the boundary
   * @return the boundary
   */
  @Nullable
  protected static BigDecimal getDecimalBoundary(@Nullable final BigDecimal d,
      @Nullable final Integer scale, @Nonnull final BiFunction<String, Integer, String> function) {
    if (d == null || scale == null) {
      return null;
    }
    final BigDecimal scaled = d.setScale(scale, RoundingMode.UNNECESSARY);
    final String result = function.apply(scaled.toPlainString(), scale + 1);
    final MathContext mathContext = new MathContext(DecimalCustomCoder.precision());
    return new BigDecimal(result, mathContext);
  }

}
