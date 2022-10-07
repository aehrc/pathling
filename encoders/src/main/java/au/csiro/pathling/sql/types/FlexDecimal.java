package au.csiro.pathling.sql.types;

import java.math.BigDecimal;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class FlexDecimal {

  public static int MAX_PRECISION = 48;

  @Nonnull
  public static DataType DATA_TYPE = DataTypes.StringType;

  @Nonnull
  private static UserDefinedFunction toBooleanUdf(
      UDF2<BigDecimal, BigDecimal, Boolean> method) {
    final UDF2<String, String, Boolean> f = (left, right) ->
        (left == null || right == null)
        ? null
        : method.call(fromValue(left), fromValue(right));
    return functions.udf(f, DataTypes.BooleanType);
  }

  @Nonnull
  private static UDF2<String, String, String> wrapBigDecimal2(
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

  @Nullable
  public static BigDecimal fromValue(@Nullable final String value) {
    return value != null
           ? new BigDecimal(value)
           : null;
  }

  @Nullable
  public static String toValue(@Nullable final BigDecimal decimal) {
    return decimal != null && decimal.precision() <= MAX_PRECISION
           ? decimal.toPlainString()
           : null;
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
}
