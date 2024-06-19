package au.csiro.pathling.sql.dates.time;

import au.csiro.pathling.sql.udf.SqlFunction1;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * A function for normalizing a time string to a long (nanoseconds) for comparison purposes.
 * <p>
 * Truncates the precision of the time to milliseconds, as this is the highest precision supported
 * by FHIR.
 */
public class NormalizeTimeFunction implements SqlFunction1<String, Long> {

  private static final long serialVersionUID = 4117774962772392910L;

  public static final String FUNCTION_NAME = "normalize_time";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.LongType;
  }

  @Override
  public Long call(final String timeString) throws Exception {
    final LocalTime time = Objects.requireNonNull(
        LocalTime.parse(timeString));
    return time.truncatedTo(ChronoUnit.MILLIS).toNanoOfDay();
  }

}
