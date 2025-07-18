package au.csiro.pathling.sql.misc;

import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.utilities.DateTimeUtil;

public class HighBoundaryForDate implements SqlFunction1<String, String> {

  public static final String FUNCTION_NAME = "high_boundary_for_date";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.StringType;
  }

  @Nullable
  @Override
  public String call(@Nullable final String s) throws Exception {
    if (s == null) {
      return null;
    }
    return DateTimeUtil.highBoundaryForDate(s, 17);
  }
}
