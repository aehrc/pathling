package au.csiro.pathling.fhirpath.column;

import java.util.Optional;
import javax.annotation.Nonnull;
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

  public static final String SCALE_SUFFIX = "_scale";
  private final Optional<Column> scaleValue;

  @Nonnull
  public static DecimalRepresentation fromTraversal(
      @Nonnull final DefaultRepresentation column, @Nonnull final String fieldName) {
    return new DecimalRepresentation(column.traverseColumn(fieldName),
        column.traverseColumn(fieldName + SCALE_SUFFIX));
  }

  public DecimalRepresentation(@Nonnull final Column value) {
    super(value);
    this.scaleValue = Optional.empty();
  }

  public DecimalRepresentation(@Nonnull final Column value, @Nonnull final Column scaleValue) {
    super(value);
    this.scaleValue = Optional.of(scaleValue);
  }

}
