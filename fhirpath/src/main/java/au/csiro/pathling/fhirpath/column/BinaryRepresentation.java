package au.csiro.pathling.fhirpath.column;

import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Describes a representation of a binary value, which is stored as a byte array but surfaced as a
 * String in FHIRPath.
 *
 * @author John Grimes
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class BinaryRepresentation extends DefaultRepresentation {

  /**
   * @param value The value to represent
   */
  public BinaryRepresentation(final Column value) {
    super(value);
  }

  @Override
  @Nonnull
  public Column getValue() {
    return new DefaultRepresentation(super.getValue()).transform(functions::base64).getValue();
  }

  @Override
  @Nonnull
  protected BinaryRepresentation copyOf(@Nonnull final Column newValue) {
    return new BinaryRepresentation(newValue);
  }

}
