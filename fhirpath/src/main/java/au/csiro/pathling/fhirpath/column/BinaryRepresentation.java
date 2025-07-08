package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.encoders.ValueFunctions;
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
  private BinaryRepresentation(final Column value) {
    super(value);
  }

  /**
   * Creates a new BinaryRepresentation from a binary-typed column.
   *
   * @param column a column containing binary data
   * @return a new instance of BinaryRepresentation
   */
  @Nonnull
  public static BinaryRepresentation fromBinaryColumn(@Nonnull final Column column) {
    return new BinaryRepresentation(
        ValueFunctions.ifArray(column, c -> functions.transform(c, functions::base64),
            functions::base64));
  }

  @Override
  @Nonnull
  protected BinaryRepresentation copyOf(@Nonnull final Column newValue) {
    return new BinaryRepresentation(newValue);
  }

}
