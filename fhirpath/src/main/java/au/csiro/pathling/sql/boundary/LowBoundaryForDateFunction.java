package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.sql.udf.SqlFunction2;
import jakarta.annotation.Nullable;
import java.util.Objects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.utilities.Utilities;

/**
 * UDF to calculate the low boundary for a date.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/HL7/FHIRPath/#lowboundaryprecision-integer-decimal--date--datetime--time">lowBoundary</a>
 */
public class LowBoundaryForDateFunction extends DateTimeBoundaryFunction implements
    SqlFunction2<String, Integer, String> {

  private static final long serialVersionUID = 1348222696516090739L;

  @Override
  public String getName() {
    return "low_boundary_for_date";
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.StringType;
  }

  @Override
  @Nullable
  public String call(@Nullable final String s, @Nullable final Integer precision) throws Exception {
    if (s == null) {
      return null;
    }
    return Utilities.lowBoundaryForDate(s,
        Objects.requireNonNullElse(precision, DATE_BOUNDARY_PRECISION));
  }

}