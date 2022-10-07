package au.csiro.pathling.sql.types;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import org.fhir.ucum.Decimal;
import org.fhir.ucum.UcumException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;

public class UcumDecimal {

  @Nonnull
  public static DataType DATA_TYPE = DataTypes.StringType;

  @Nonnull
  private static UserDefinedFunction toBooleanUdf(
      UDF2<Decimal, Decimal, Boolean> method) {
    final UDF2<String, String, Boolean> f = (left, right) ->
        method.call(fromValue(left), fromValue(right));
    return functions.udf(f, DataTypes.BooleanType);
  }

  @Nonnull
  private static UserDefinedFunction toBigDecimalUdf(
      UDF2<Decimal, Decimal, Decimal> method) {

    final UDF2<String, String, String> f = (left, right) ->
        toValue(method.call(fromValue(left), fromValue(right)));
    return functions.udf(f, DATA_TYPE);
  }

  @Nullable
  public static Decimal fromValue(@Nonnull final String value) {
    try {
      return new Decimal(value);
    } catch (UcumException e) {
      return null;
    }
  }

  @Nonnull
  public static String toValue(@Nonnull final Decimal decimal) {
    return decimal.toString();
  }

  private static final UserDefinedFunction EQUALS_UDF = toBooleanUdf(Decimal::equals);
  private static final UserDefinedFunction LT_UDF = toBooleanUdf((l, r) -> l.comparesTo(r) < 0);

  private static final UserDefinedFunction PLUS_UDF = toBigDecimalUdf(Decimal::add);
  private static final UserDefinedFunction MULTIPLY_UDF = toBigDecimalUdf(Decimal::multiply);

  @Nonnull
  public static Column equals(@Nonnull final Column left, @Nonnull final Column right) {
    return EQUALS_UDF.apply(left, right);
  }

  @Nonnull
  public static Column lt(@Nonnull final Column left, @Nonnull final Column right) {
    return LT_UDF.apply(left, right);
  }

  @Nonnull
  public static Column plus(@Nonnull final Column left, @Nonnull final Column right) {
    return PLUS_UDF.apply(left, right);
  }

  @Nonnull
  public static Column multiply(@Nonnull final Column left, @Nonnull final Column right) {
    return MULTIPLY_UDF.apply(left, right);
  }
}
