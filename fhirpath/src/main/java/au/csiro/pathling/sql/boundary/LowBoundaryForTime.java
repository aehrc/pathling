package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.sql.udf.SqlFunction1;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.utilities.Utilities;

/**
 * UDF to calculate the low boundary for a time.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#functions">FHIRPath - Additional functions</a>
 */
public class LowBoundaryForTime extends DateTimeBoundaryFunction implements
    SqlFunction1<String, String> {

  private static final long serialVersionUID = -1441575791985398397L;

  @Override
  public String getName() {
    return "low_boundary_for_time";
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
    return Utilities.lowBoundaryForTime(s, TIME_BOUNDARY_PRECISION);
  }

}
