package au.csiro.pathling.sql.boundary;

import au.csiro.pathling.sql.udf.SqlFunction1;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.utilities.Utilities;

public class LowBoundaryForDateFunction implements SqlFunction1<String, String> {

  private static final long serialVersionUID = 6447986507343189426L;

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
  public String call(@Nullable final String s) throws Exception {
    if (s == null) {
      return null;
    }
    return Utilities.lowBoundaryForDate(s, 17);
  }

}
