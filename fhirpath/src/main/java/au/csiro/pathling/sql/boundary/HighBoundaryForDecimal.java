package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.sql.udf.SqlFunction2;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.utilities.Utilities;

/**
 * UDF to calculate the high boundary for a decimal.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#functions">FHIRPath - Additional functions</a>
 */
public class HighBoundaryForDecimal extends DecimalBoundaryFunction implements
    SqlFunction2<BigDecimal, Integer, BigDecimal> {

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
  public BigDecimal call(@Nullable final BigDecimal d, @Nullable final Integer scale)
      throws Exception {
    return getDecimalBoundary(d, scale, Utilities::highBoundaryForDecimal);
  }

}