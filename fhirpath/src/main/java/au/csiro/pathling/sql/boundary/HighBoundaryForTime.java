package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.sql.udf.SqlFunction1;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.utilities.Utilities;

public class HighBoundaryForTime implements SqlFunction1<String, String> {

  private static final long serialVersionUID = 1198815979308332180L;

  @Override
  public String getName() {
    return "high_boundary_for_time";
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
    return Utilities.highBoundaryForTime(s, 9);
  }

}
