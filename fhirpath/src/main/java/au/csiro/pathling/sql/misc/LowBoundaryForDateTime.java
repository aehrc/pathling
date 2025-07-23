package au.csiro.pathling.sql.misc;

import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

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
    
    try {
      // Handle different FHIR date/dateTime formats
      if (s.matches("\\d{4}")) {
        // Year only: YYYY -> start of year
        final int year = Integer.parseInt(s);
        return Timestamp.from(LocalDateTime.of(year, 1, 1, 0, 0, 0, 0)
            .toInstant(ZoneOffset.UTC));
      } else if (s.matches("\\d{4}-\\d{2}")) {
        // Year-Month: YYYY-MM -> start of month
        final LocalDate date = LocalDate.parse(s + "-01");
        return Timestamp.from(LocalDateTime.of(date, LocalDateTime.MIN.toLocalTime())
            .toInstant(ZoneOffset.UTC));
      } else if (s.matches("\\d{4}-\\d{2}-\\d{2}")) {
        // Date only: YYYY-MM-DD -> start of day
        final LocalDate date = LocalDate.parse(s);
        return Timestamp.from(LocalDateTime.of(date, LocalDateTime.MIN.toLocalTime())
            .toInstant(ZoneOffset.UTC));
      } else {
        // Full dateTime with timezone -> parse as-is
        return Timestamp.from(Instant.parse(s));
      }
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("Invalid date/dateTime format: " + s, e);
    }
  }
}
