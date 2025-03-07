package au.csiro.pathling.sql.misc;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.fhirpath.CalendarDurationUtils;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.math.BigDecimal;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Quantity;

/**
 * Spark UDF to convert a Quantity represented as a Row to a valid Quantity literal string.
 * <p>
 * UCUM units are quoted with single quotes, while time duration units are not quoted. For other
 * systems, the function returns null.
 * <p>
 * If the quantity is null, the function returns null.
 */
public class QuantityToLiteral implements SqlFunction1<Row, String> {

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "quantity_to_literal";

  @Serial
  private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.StringType;
  }

  @Override
  @Nullable
  public String call(@Nullable final Row row) {
    if (row == null) {
      return null;
    }

    // Decode the row into a Quantity object using QuantityEncoding
    final Quantity quantity = QuantityEncoding.decode(requireNonNull(row));
    @Nullable final BigDecimal value = quantity.getValue();
    @Nullable final String system = quantity.getSystem();
    @Nullable final String unit = quantity.getCode();

    if (value == null || system == null || unit == null) {
      return null;
    }

    if (quantity.getSystem().equals(Ucum.SYSTEM_URI)) {
      // UCUM units are quoted
      return String.format("%s '%s'", value.toPlainString(), unit);
    } else if (CalendarDurationUtils.isCalendarDuration(quantity)) {
      // Time duration units are not quoted
      return String.format("%s %s", value.toPlainString(), unit);
    } else {
      // For other systems, return null
      return null;
    }
  }
}
