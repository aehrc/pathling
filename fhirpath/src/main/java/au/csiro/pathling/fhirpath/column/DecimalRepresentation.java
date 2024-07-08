package au.csiro.pathling.fhirpath.column;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.spark.sql.Column;

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
    return new DecimalRepresentation(column.traverseColumn(fieldName),
        column.traverseColumn(fieldName + SCALE_SUFFIX));
  }

  /**
   * @param value The value to represent
   */
  public DecimalRepresentation(@Nonnull final Column value) {
    super(value);
    this.scaleValue = Optional.empty();
  }

  /**
   * @param value The value to represent
   * @param scaleValue The original scale of the value
   */
  public DecimalRepresentation(@Nonnull final Column value, @Nonnull final Column scaleValue) {
    super(value);
    this.scaleValue = Optional.of(scaleValue);
  }

}
