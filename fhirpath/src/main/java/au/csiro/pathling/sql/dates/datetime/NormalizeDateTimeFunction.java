package au.csiro.pathling.sql.dates.datetime;

import au.csiro.pathling.sql.misc.TemporalDifferenceFunction;
import au.csiro.pathling.sql.udf.SqlFunction1;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * A function for normalizing a datetime string to an instant for comparison purposes.
 * <p>
 * Truncates the precision of the datetime to milliseconds, as this is the highest precision
 * supported by FHIR.
 */
public class NormalizeDateTimeFunction implements SqlFunction1<String, Instant> {
 
  private static final long serialVersionUID = 8950454995339187419L;

  public static final String FUNCTION_NAME = "normalize_datetime";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.TimestampType;
  }

  @Override
  public Instant call(final String dateTimeString) throws Exception {
    final ZonedDateTime dateTime = Objects.requireNonNull(
        TemporalDifferenceFunction.parse(dateTimeString));
    return dateTime.truncatedTo(ChronoUnit.MILLIS).toInstant();
  }

}
