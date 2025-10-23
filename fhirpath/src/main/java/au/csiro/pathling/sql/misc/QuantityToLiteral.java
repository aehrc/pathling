package au.csiro.pathling.sql.misc;

import static au.csiro.pathling.fhirpath.FhirpathQuantity.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI;
import static au.csiro.pathling.fhirpath.FhirpathQuantity.UCUM_SYSTEM_URI;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

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
    @Nullable final String code = quantity.getCode();
    @Nullable final String unit = quantity.getUnit();

    if (value == null || system == null || code == null) {
      return null;
    }
    if (UCUM_SYSTEM_URI.equals(system)) {
      // UCUM units are quoted
      return String.format("%s '%s'", value.toPlainString(), code);
    } else if (FHIRPATH_CALENDAR_DURATION_SYSTEM_URI.equals(system)) {
      // Time duration units are not quoted
      return String.format("%s %s", value.toPlainString(),
          nonNull(unit)
          ? unit
          : code);
    } else {
      // For other systems, return null
      return null;
    }
  }
}
