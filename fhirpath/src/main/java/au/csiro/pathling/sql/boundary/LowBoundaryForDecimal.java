package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.sql.udf.SqlFunction2;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.utilities.Utilities;

public class LowBoundaryForDecimal implements SqlFunction2<BigDecimal, Integer, BigDecimal> {

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
  public BigDecimal call(@Nullable final BigDecimal d, @Nullable final Integer scale)
      throws Exception {
    if (d == null || scale == null) {
      return null;
    }
    final BigDecimal scaled = d.setScale(scale, RoundingMode.UNNECESSARY);
    final String result = Utilities.lowBoundaryForDecimal(scaled.toPlainString(), scale + 1);
    final MathContext mathContext = new MathContext(DecimalCustomCoder.precision());
    return new BigDecimal(result, mathContext);
  }

}
