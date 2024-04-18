package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.sql.udf.SqlFunction1;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.utilities.Utilities;

/**
 * UDF to calculate the high boundary for a date time.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#functions">FHIRPath - Additional functions</a>
 */
public class HighBoundaryForDateTimeFunction extends DateTimeBoundaryFunction implements
    SqlFunction1<String, String> {

  private static final long serialVersionUID = 2072364365809854318L;

  @Override
  public String getName() {
    return "high_boundary_for_date_time";
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.StringType;
  }

  @Override
  @Nullable
  public String call(@Nullable final String s) throws Exception {
    if (s == null) {
      return null;
    }
    return Utilities.highBoundaryForDate(s, DATE_TIME_BOUNDARY_PRECISION);
  }

}
