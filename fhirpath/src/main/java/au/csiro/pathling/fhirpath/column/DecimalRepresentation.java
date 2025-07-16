package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.sql.misc.DecimalToLiteral;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Describes a representation of a decimal value, which includes:
 * <ul>
 *   <li>The value itself</li>
 *   <li>The original scale of the value</li>
 * </ul>
 *
 * @author John Grimes
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class DecimalRepresentation extends DefaultRepresentation {

  /**
   * The column name suffix used to store the original scale of the decimal value.
   */
  public static final String SCALE_SUFFIX = "_scale";


  private final Optional<Column> scaleValue;

  /**
   * Build a new {@link DecimalRepresentation} from a parent representation and a field name.
   *
   * @param column The parent representation
   * @param fieldName The field name
   * @return A new {@link DecimalRepresentation}
   */
  @Nonnull
  public static DecimalRepresentation fromTraversal(
      @Nonnull final DefaultRepresentation column, @Nonnull final String fieldName) {
    return new DecimalRepresentation(column.traverse(fieldName).getValue(),
        column.traverseColumn(fieldName + SCALE_SUFFIX));
  }

  @Override
  public DecimalRepresentation copyOf(@Nonnull final Column newValue) {
    return scaleValue.map(scale -> new DecimalRepresentation(newValue, scale))
        .orElseGet(() -> new DecimalRepresentation(newValue));
  }

  /**
   * @param value The value to represent
   */
  public DecimalRepresentation(@Nonnull final Column value) {
    super(value);
    this.scaleValue = Optional.empty();
  }

  public DecimalRepresentation(@Nonnull final BigDecimal value) {
    this(functions.lit(value), functions.lit(value.scale()));
  }

  /**
   * @param value The value to represent
   * @param scaleValue The original scale of the value
   */
  public DecimalRepresentation(@Nonnull final Column value, @Nonnull final Column scaleValue) {
    super(value);
    this.scaleValue = Optional.of(scaleValue);
  }

  @Override
  @Nonnull
  public ColumnRepresentation asString() {
    return transformWithUdf(DecimalToLiteral.FUNCTION_NAME,
        scaleValue.map(DefaultRepresentation::new)
            .orElse(DefaultRepresentation.empty()));
  }
}
