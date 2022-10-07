package au.csiro.pathling.sql.types;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import javax.annotation.Nonnull;
import java.math.BigDecimal;

public class FlexiDecimal {

  @Nonnull
  private static StructType createFlexibleDecimalType() {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField value = new StructField("value", DataTypes.createDecimalType(32, 0), true,
        metadata);
    final StructField scale = new StructField("scale", DataTypes.IntegerType, true, metadata);
    return new StructType(new StructField[]{value, scale});
  }

  @Nonnull
  public static DataType DATA_TYPE = createFlexibleDecimalType();

  @Nonnull
  private static UserDefinedFunction toBooleanUdf(
      UDF2<BigDecimal, BigDecimal, Boolean> method) {
    final UDF2<Row, Row, Boolean> f = (left, right) ->
        method.call(fromValue(left), fromValue(right));
    return functions.udf(f, DataTypes.BooleanType);
  }

  @Nonnull
  private static UDF2<Row, Row, Row> wrapBigDecimal2(
      UDF2<BigDecimal, BigDecimal, BigDecimal> method) {
    return (left, right) -> toValue(
        method.call(fromValue(left), fromValue(right)));
  }

  @Nonnull
  private static UserDefinedFunction toBigDecimalUdf(
      UDF2<BigDecimal, BigDecimal, BigDecimal> method) {
    return functions.udf(wrapBigDecimal2(method), DATA_TYPE);
  }

  @Nonnull
  public static BigDecimal fromValue(@Nonnull final Row row) {
    final BigDecimal unscaledValue = row.getDecimal(0);
    return unscaledValue.movePointLeft(row.getInt(1));
  }

  @Nonnull
  public static Row toValue(@Nonnull final BigDecimal decimal) {
    return RowFactory.create(decimal.movePointRight(decimal.scale()), decimal.scale());
  }

  private static final UserDefinedFunction EQUALS_UDF = toBooleanUdf(BigDecimal::equals);
  private static final UserDefinedFunction LT_UDF = toBooleanUdf((l, r) -> l.compareTo(r) < 0);

  private static final UserDefinedFunction PLUS_UDF = toBigDecimalUdf(BigDecimal::add);
  private static final UserDefinedFunction MULTIPLY_UDF = toBigDecimalUdf(BigDecimal::multiply);

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
