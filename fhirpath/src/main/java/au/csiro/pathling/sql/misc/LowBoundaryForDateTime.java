package au.csiro.pathling.sql.misc;

import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.utilities.DateTimeUtil;

public class LowBoundaryForDateTime implements SqlFunction1<String, Timestamp> {
  
  @Serial
  private static final long serialVersionUID = -2161361690351000200L;

  public static final String FUNCTION_NAME = "low_boundary_for_date";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.TimestampType;
  }

  @Nullable
  @Override
  public Timestamp call(@Nullable final String s) throws Exception {
    if (s == null) {
      return null;
    }
    final String stringResult = DateTimeUtil.lowBoundaryForDate(s, 17);
    return Timestamp.from(Instant.parse(stringResult));
  }
}
