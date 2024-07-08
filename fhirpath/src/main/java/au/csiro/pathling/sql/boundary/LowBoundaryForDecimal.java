package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.sql.udf.SqlFunction2;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.utilities.Utilities;

/**
 * UDF to calculate the low boundary for a decimal.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/HL7/FHIRPath/#lowboundaryprecision-integer-decimal--date--datetime--time">lowBoundary</a>
 */
public class LowBoundaryForDecimal extends DecimalBoundaryFunction implements
    SqlFunction2<BigDecimal, Integer, BigDecimal> {

  private static final long serialVersionUID = -1470191359168496892L;

  @Override
  public String getName() {
    return "low_boundary_for_decimal";
  }

  @Override
  public DataType getReturnType() {
    return DecimalCustomCoder.decimalType();
  }

  @Override
  @Nullable
  public BigDecimal call(@Nullable final BigDecimal d, @Nullable final Integer precision)
      throws Exception {
    if (d == null || precision == null) {
      return null;
    }
    final String result = Utilities.lowBoundaryForDecimal(d.toPlainString(), precision);
    return new BigDecimal(result);
  }

}
