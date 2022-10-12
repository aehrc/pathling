package au.csiro.pathling.sql.types;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Implementation of flexible decimal type represented as the unscaled value with up to 38 digits
 * and the scale.
 */
public class FlexiDecimal {

  /**
   * The maximum precision (the number of significant digits)
   */
  public static final int MAX_PRECISION = 38;
  /**
   * The Sql type of for the unscaled value.
   */
  public static final DataType DECIMAL_TYPE = DataTypes.createDecimalType(MAX_PRECISION, 0);

  @Nonnull
  private static StructType createFlexibleDecimalType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField value = new StructField("value", DECIMAL_TYPE, true,
        metadata);
    final StructField scale = new StructField("scale", DataTypes.IntegerType, true, metadata);
    return new StructType(new StructField[]{value, scale});
  }

  /**
   * The Sql (struct) type for flexible decimal
   */
  @Nonnull
  public static DataType DATA_TYPE = createFlexibleDecimalType();

  @Nonnull
  private static UserDefinedFunction toBooleanUdf(
      UDF2<BigDecimal, BigDecimal, Boolean> method) {
    final UDF2<Row, Row, Boolean> f = (left, right) -> {
      final BigDecimal leftValue = fromValue(left);
      final BigDecimal rightValue = fromValue(right);
      return (leftValue == null || rightValue == null)
             ? null
             : method.call(leftValue, rightValue);
    };
    return functions.udf(f, DataTypes.BooleanType);
  }

  @Nonnull
  private static UDF2<Row, Row, Row> wrapBigDecimal2(
      UDF2<BigDecimal, BigDecimal, BigDecimal> method) {
    return (left, right) ->
        (left == null || right == null)
        ? null
        : toValue(method.call(fromValue(left), fromValue(right)));
  }

  @Nonnull
  private static UserDefinedFunction toBigDecimalUdf(
      UDF2<BigDecimal, BigDecimal, BigDecimal> method) {
    return functions.udf(wrapBigDecimal2(method), DATA_TYPE);
  }

  /**
   * Decodes a flexible decimal from the Row
   *
   * @param row the row to decode
   * @return the BigDecmal representation of the row
   */
  @Nullable
  public static BigDecimal fromValue(@Nullable final Row row) {
    return row != null && !row.isNullAt(0)
           ? row.getDecimal(0).movePointLeft(row.getInt(1))
           : null;
  }

  /**
   * Encodes a flexible decimal into a Row
   *
   * @param decimal the decimal to encode
   * @return the Row representation of the decimal
   */
  @Nullable
  public static Row toValue(@Nullable final BigDecimal decimal) {
    final Object[] fieldValues = toArrayValue(decimal);
    return fieldValues != null
           ? RowFactory.create(fieldValues)
           : null;
  }

  @Nullable
  private static Object[] toArrayValue(@Nullable final BigDecimal decimal) {
    BigDecimal normalizedValue = normalize(decimal);
    return normalizedValue != null
           ? new Object[]{Decimal.apply(normalizedValue.unscaledValue()), normalizedValue.scale()}
           : null;
  }

  @Nullable
  public static BigDecimal normalize(@Nullable final BigDecimal decimal) {
    if (decimal == null) {
      return null;
    } else {
      final BigDecimal adjustedValue = decimal.scale() < 0
                                       ? decimal.setScale(0, RoundingMode.UNNECESSARY)
                                       : decimal;
      // This may be may have too many digits
      if (adjustedValue.precision() > MAX_PRECISION) {
        // we need to adjust the scale to fit into the desired precision
        int desiredScale = adjustedValue.scale() - (adjustedValue.precision() - MAX_PRECISION);
        if (desiredScale >= 0) {
          return adjustedValue.setScale(desiredScale, RoundingMode.HALF_UP);
        } else {
          return null;
        }
      } else {
        return adjustedValue;
      }
    }
  }

  private static final UserDefinedFunction EQUALS_UDF = toBooleanUdf((l, r) -> l.compareTo(r) == 0);
  private static final UserDefinedFunction LT_UDF = toBooleanUdf((l, r) -> l.compareTo(r) < 0);
  private static final UserDefinedFunction LTE_UDF = toBooleanUdf((l, r) -> l.compareTo(r) <= 0);
  private static final UserDefinedFunction GT_UDF = toBooleanUdf((l, r) -> l.compareTo(r) > 0);
  private static final UserDefinedFunction GTE_UDF = toBooleanUdf((l, r) -> l.compareTo(r) >= 0);

  private static final UserDefinedFunction PLUS_UDF = toBigDecimalUdf(BigDecimal::add);
  private static final UserDefinedFunction MULTIPLY_UDF = toBigDecimalUdf(BigDecimal::multiply);
  private static final UserDefinedFunction MINUS_UDF = toBigDecimalUdf(BigDecimal::subtract);
  private static final UserDefinedFunction DIVIDE_UDF = toBigDecimalUdf(BigDecimal::divide);

  private static final UserDefinedFunction TO_DECIMAL = functions.udf(
      (UDF1<Row, BigDecimal>) FlexiDecimal::fromValue,
      DecimalCustomCoder.decimalType());

  @Nonnull
  public static Column equals(@Nonnull final Column left, @Nonnull final Column right) {
    return EQUALS_UDF.apply(left, right);
  }

  @Nonnull
  public static Column lt(@Nonnull final Column left, @Nonnull final Column right) {
    return LT_UDF.apply(left, right);
  }

  @Nonnull
  public static Column lte(@Nonnull final Column left, @Nonnull final Column right) {
    return LTE_UDF.apply(left, right);
  }

  @Nonnull
  public static Column gt(@Nonnull final Column left, @Nonnull final Column right) {
    return GT_UDF.apply(left, right);
  }

  @Nonnull
  public static Column gte(@Nonnull final Column left, @Nonnull final Column right) {
    return GTE_UDF.apply(left, right);
  }

  @Nonnull
  public static Column plus(@Nonnull final Column left, @Nonnull final Column right) {
    return PLUS_UDF.apply(left, right);
  }

  @Nonnull
  public static Column multiply(@Nonnull final Column left, @Nonnull final Column right) {
    return MULTIPLY_UDF.apply(left, right);
  }

  @Nonnull
  public static Column minus(@Nonnull final Column left, @Nonnull final Column right) {
    return MINUS_UDF.apply(left, right);
  }

  @Nonnull
  public static Column divide(@Nonnull final Column left, @Nonnull final Column right) {
    return DIVIDE_UDF.apply(left, right);
  }

  @Nonnull
  public static Column to_decimal(@Nonnull final Column flexiDecimal) {
    return TO_DECIMAL.apply(flexiDecimal);
  }
}
