package au.csiro.pathling.fhirpath.column;

import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

class NullRepresentation extends ColumnRepresentation {

  static final ColumnRepresentation INSTANCE = new NullRepresentation();


  @Override
  public Column getValue() {
    return NULL_LITERAL;
  }

  @Override
  protected ColumnRepresentation copyOf(@Nonnull final Column newValue) {
    return this;
  }

  @Nonnull
  @Override
  public ColumnRepresentation vectorize(@Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return this;
  }

  @Nonnull
  @Override
  public ColumnRepresentation flatten() {
    return this;
  }

  @Nonnull
  @Override
  public ColumnRepresentation traverse(@Nonnull final String fieldName) {
    return this;
  }
}
