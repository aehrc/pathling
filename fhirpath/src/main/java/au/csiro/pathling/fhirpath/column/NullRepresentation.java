package au.csiro.pathling.fhirpath.column;

import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Describes a representation of an empty collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class NullRepresentation extends ColumnRepresentation {

  static final ColumnRepresentation INSTANCE = new NullRepresentation();
  static final Column NULL_LITERAL = functions.lit(null);

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
  public NullRepresentation traverse(@Nonnull final String fieldName) {
    return this;
  }

  @Nonnull
  @Override
  public NullRepresentation traverse(@Nonnull final String fieldName,
      final Optional<FHIRDefinedType> fhirType) {
    return traverse(fieldName);
  }

}