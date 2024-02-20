package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.sql.udf.SqlFunction1;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.utilities.Utilities;

public class HighBoundaryForDecimal implements SqlFunction1<BigDecimal, BigDecimal> {

  private static final long serialVersionUID = 347105321442978614L;

  @Override
  public String getName() {
    return "high_boundary_for_decimal";
  }

  @Override
  public DataType getReturnType() {
    return DecimalCollection.getDecimalType();
  }

  @Override
  @Nullable
  public BigDecimal call(@Nullable final BigDecimal d) throws Exception {
    if (d == null) {
      return null;
    }
    final String decimalString = d.toPlainString();
    final String result = Utilities.highBoundaryForDecimal(decimalString,
        DecimalCustomCoder.precision());
    return new BigDecimal(result);
  }

}
