package au.csiro.pathling.sql.udf;

import au.csiro.pathling.spark.SparkConfigurer;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.SparkSession;

/**
 * A spark configurer that registers user defined functions in the sessions.
 */
public class SqlFunctionRegistrar implements SparkConfigurer {

  @Nonnull
  private final List<SqlFunction1<?, ?>> sqlFunction1;
  @Nonnull
  private final List<SqlFunction2<?, ?, ?>> sqlFunction2;
  @Nonnull
  private final List<SqlFunction3<?, ?, ?, ?>> sqlFunction3;
  @Nonnull
  private final List<SqlFunction4<?, ?, ?, ?, ?>> sqlFunction4;

  @Nonnull
  private final List<SqlFunction5<?, ?, ?, ?, ?, ?>> sqlFunction5;

  public SqlFunctionRegistrar(
      @Nonnull final List<SqlFunction1<?, ?>> sqlFunction1,
      @Nonnull final List<SqlFunction2<?, ?, ?>> sqlFunction2,
      @Nonnull final List<SqlFunction3<?, ?, ?, ?>> sqlFunction3,
      @Nonnull final List<SqlFunction4<?, ?, ?, ?, ?>> sqlFunction4,
      @Nonnull final List<SqlFunction5<?, ?, ?, ?, ?, ?>> sqlFunction5
  ) {
    this.sqlFunction1 = sqlFunction1;
    this.sqlFunction2 = sqlFunction2;
    this.sqlFunction3 = sqlFunction3;
    this.sqlFunction4 = sqlFunction4;
    this.sqlFunction5 = sqlFunction5;
  }

  @Override
  public void configure(@Nonnull SparkSession spark) {
    for (final SqlFunction1<?, ?> function : sqlFunction1) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction2<?, ?, ?> function : sqlFunction2) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction3<?, ?, ?, ?> function : sqlFunction3) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction4<?, ?, ?, ?, ?> function : sqlFunction4) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction5<?, ?, ?, ?, ?, ?> function : sqlFunction5) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
  }
}
