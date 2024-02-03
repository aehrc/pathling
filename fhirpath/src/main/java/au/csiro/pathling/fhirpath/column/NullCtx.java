package au.csiro.pathling.fhirpath.column;

import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

class NullCtx extends ColumnCtx {

  static final ColumnCtx INSTANCE = new au.csiro.pathling.fhirpath.column.NullCtx();


  @Override
  public Column getValue() {
    return NULL_LITERAL;
  }

  @Override
  protected ColumnCtx copyOf(@Nonnull final Column newValue) {
    return this;
  }

  @Nonnull
  @Override
  public ColumnCtx vectorize(@Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return this;
  }

  @Nonnull
  @Override
  public ColumnCtx flatten() {
    return this;
  }

  @Nonnull
  @Override
  public ColumnCtx traverse(@Nonnull final String fieldName) {
    return this;
  }
}
