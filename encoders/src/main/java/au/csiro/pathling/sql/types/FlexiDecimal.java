/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package au.csiro.pathling.sql.types;

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Implementation of flexible decimal type represented as the unscaled value with up to 38 digits
 * and the scale.
 *
 * @author Piotr Szul
 */
@SuppressWarnings("unused")
public final class FlexiDecimal {

  private FlexiDecimal() {}

  /** The maximum precision (the number of significant digits). */
  public static final int MAX_PRECISION = 38;

  /** The SQL type for the unscaled value. */
  public static final DataType DECIMAL_TYPE = DataTypes.createDecimalType(MAX_PRECISION, 0);

  @Nonnull
  private static StructType createFlexibleDecimalType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField value = new StructField("value", DECIMAL_TYPE, true, metadata);
    final StructField scale = new StructField("scale", DataTypes.IntegerType, true, metadata);
    return new StructType(new StructField[] {value, scale});
  }

  /** The SQL (struct) type for flexible decimal. */
  @Nonnull public static final DataType DATA_TYPE = createFlexibleDecimalType();

  @Nonnull
  private static UserDefinedFunction toBooleanUdf(
      @Nonnull final UDF2<BigDecimal, BigDecimal, Boolean> method) {
    final UDF2<Row, Row, Boolean> f =
        (left, right) -> {
          final BigDecimal leftValue = fromValue(left);
          final BigDecimal rightValue = fromValue(right);
          //noinspection ReturnOfNull
          return (leftValue == null || rightValue == null)
              ? null
              : method.call(leftValue, rightValue);
        };
    return functions.udf(f, DataTypes.BooleanType);
  }

  @Nonnull
  private static UDF2<Row, Row, Row> wrapBigDecimal2(
      @Nonnull final UDF2<BigDecimal, BigDecimal, BigDecimal> method) {
    //noinspection ReturnOfNull
    return (left, right) ->
        (left == null || right == null)
            ? null
            : toValue(method.call(fromValue(left), fromValue(right)));
  }

  @Nonnull
  private static UserDefinedFunction toBigDecimalUdf(
      @Nonnull final UDF2<BigDecimal, BigDecimal, BigDecimal> method) {
    return functions.udf(wrapBigDecimal2(method), DATA_TYPE);
  }

  /**
   * Decodes a flexible decimal from a Row.
   *
   * @param row the row to decode
   * @return the BigDecimal representation of the row
   */
  @Nullable
  public static BigDecimal fromValue(@Nullable final Row row) {
    return row != null && !row.isNullAt(0) ? row.getDecimal(0).movePointLeft(row.getInt(1)) : null;
  }

  /**
   * Encodes a flexible decimal into a Row.
   *
   * @param decimal the decimal to encode
   * @return the Row representation of the decimal
   */
  @Nullable
  public static Row toValue(@Nullable final BigDecimal decimal) {
    final Object[] fieldValues = toArrayValue(decimal);
    return fieldValues != null ? RowFactory.create(fieldValues) : null;
  }

  @Nullable
  private static Object[] toArrayValue(@Nullable final BigDecimal decimal) {
    final BigDecimal normalizedValue = normalize(decimal);
    return normalizedValue != null
        ? new Object[] {Decimal.apply(normalizedValue.unscaledValue()), normalizedValue.scale()}
        : null;
  }

  @Nullable
  private static Row negate(@Nullable final Row row) {
    final BigDecimal value = fromValue(row);
    return value == null ? null : toValue(value.negate());
  }

  /**
   * Negates (applies unary `-`) the value of the specified flexible decimal.
   *
   * @param flexiDecimal the flexible decimal to negate
   * @return the negated value
   */
  @Nonnull
  public static Column negate(@Nonnull final Column flexiDecimal) {
    return NEGATE_UDF.apply(flexiDecimal);
  }

  /**
   * Normalises a BigDecimal to fit within the maximum precision constraints.
   *
   * @param decimal the decimal to normalise
   * @return the normalised decimal, or null if it cannot be represented
   */
  @Nullable
  public static BigDecimal normalize(@Nullable final BigDecimal decimal) {
    if (decimal == null) {
      return null;
    } else {
      final BigDecimal adjustedValue =
          decimal.scale() < 0 ? decimal.setScale(0, RoundingMode.UNNECESSARY) : decimal;
      // This may be may have too many digits
      if (adjustedValue.precision() > MAX_PRECISION) {
        // we need to adjust the scale to fit into the desired precision
        final int desiredScale =
            adjustedValue.scale() - (adjustedValue.precision() - MAX_PRECISION);
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

  private static final UserDefinedFunction TO_DECIMAL =
      functions.udf(
          (UDF1<Row, BigDecimal>) FlexiDecimal::fromValue, DecimalCustomCoder.decimalType());

  private static final UserDefinedFunction NEGATE_UDF =
      functions.udf((UDF1<Row, Row>) FlexiDecimal::negate, DATA_TYPE);

  /**
   * Compares two flexible decimal columns for equality.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the comparison result
   */
  @Nonnull
  public static Column equalTo(@Nonnull final Column left, @Nonnull final Column right) {
    return EQUALS_UDF.apply(left, right);
  }

  /**
   * Compares two flexible decimal columns for less than.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the comparison result
   */
  @Nonnull
  public static Column lt(@Nonnull final Column left, @Nonnull final Column right) {
    return LT_UDF.apply(left, right);
  }

  /**
   * Compares two flexible decimal columns for less than or equal to.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the comparison result
   */
  @Nonnull
  public static Column leq(@Nonnull final Column left, @Nonnull final Column right) {
    return LTE_UDF.apply(left, right);
  }

  /**
   * Compares two flexible decimal columns for greater than.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the comparison result
   */
  @Nonnull
  public static Column gt(@Nonnull final Column left, @Nonnull final Column right) {
    return GT_UDF.apply(left, right);
  }

  /**
   * Compares two flexible decimal columns for greater than or equal to.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the comparison result
   */
  @Nonnull
  public static Column geq(@Nonnull final Column left, @Nonnull final Column right) {
    return GTE_UDF.apply(left, right);
  }

  /**
   * Adds two flexible decimal columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the addition result
   */
  @Nonnull
  public static Column plus(@Nonnull final Column left, @Nonnull final Column right) {
    return PLUS_UDF.apply(left, right);
  }

  /**
   * Multiplies two flexible decimal columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the multiplication result
   */
  @Nonnull
  public static Column multiply(@Nonnull final Column left, @Nonnull final Column right) {
    return MULTIPLY_UDF.apply(left, right);
  }

  /**
   * Subtracts two flexible decimal columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the subtraction result
   */
  @Nonnull
  public static Column minus(@Nonnull final Column left, @Nonnull final Column right) {
    return MINUS_UDF.apply(left, right);
  }

  /**
   * Divides two flexible decimal columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column containing the division result
   */
  @Nonnull
  public static Column divide(@Nonnull final Column left, @Nonnull final Column right) {
    return DIVIDE_UDF.apply(left, right);
  }

  /**
   * Converts a flexible decimal column to a standard decimal column.
   *
   * @param flexiDecimal the flexible decimal column to convert
   * @return a column containing the converted decimal values
   */
  @Nonnull
  public static Column toDecimal(@Nonnull final Column flexiDecimal) {
    return TO_DECIMAL.apply(flexiDecimal);
  }
}
