package au.csiro.pathling.fhirpath.column;

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
public class DecimalRepresentation extends ArrayOrSingularRepresentation {

  public static final String SCALE_SUFFIX = "_scale";
  private final Column scaleValue;

  @Nonnull
  public static DecimalRepresentation fromTraversal(
      @Nonnull final ArrayOrSingularRepresentation column, @Nonnull final String fieldName) {
    return new DecimalRepresentation(column.traverseColumn(fieldName),
        column.traverseColumn(fieldName + SCALE_SUFFIX));
  }

  public DecimalRepresentation(@Nonnull final Column value, @Nonnull final Column scaleValue) {
    super(value);
    this.scaleValue = scaleValue;
  }

}
