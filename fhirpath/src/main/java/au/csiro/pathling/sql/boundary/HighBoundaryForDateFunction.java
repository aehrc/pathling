package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.sql.udf.SqlFunction1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.utilities.Utilities;

public class HighBoundaryForDateFunction implements SqlFunction1<String, String> {

  private static final long serialVersionUID = 2072364365809854318L;

  @Override
  public String getName() {
    return "high_boundary_for_date";
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.StringType;
  }

  @Override
  public String call(final String s) throws Exception {
    return Utilities.highBoundaryForDate(s, 17);
  }

}
