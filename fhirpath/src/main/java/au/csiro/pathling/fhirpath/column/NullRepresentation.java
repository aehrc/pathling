package au.csiro.pathling.fhirpath.column;

import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Describes a representation of an empty collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class NullRepresentation extends ColumnRepresentation {

  static final ColumnRepresentation INSTANCE = new NullRepresentation();

  /**
   * @return a singleton instance of this class
   */
  @Nonnull
  public static ColumnRepresentation getInstance() {
    return INSTANCE;
  }

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
