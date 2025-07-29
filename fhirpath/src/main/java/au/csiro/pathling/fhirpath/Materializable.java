package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.Objects;
import org.apache.spark.sql.Column;

/**
 * An interface for collections that can be converted to an external value suitable for Spark SQL
 * operations.
 * <p>
 * This interface is intended for collections that can be represented as a column in a Spark
 * DataFrame. Implementations should provide a way to convert the collection into a Spark SQL
 * column.
 * </p>
 *
 * @see Collection
 */
public interface Materializable {

  /**
   * Converts this collection to an external value that can be used in Spark SQL operations.
   * <p>
   * The default implementation returns the raw column value, but implementations can override this
   * to provide custom conversion logic.
   *
   * @return A Spark SQL column representing the external value of this collection
   */
  @Nonnull
  default Column toExternalValue() {
    return getColumn().getValue();
  }

  /**
   * Gets the column representation of this collection.
   *
   * @return The column representation of this collection
   */
  @Nonnull
  ColumnRepresentation getColumn();

  /**
   * Gets the external value of a collection.
   * <p>
   * If the collection implements {@link Materializable}, its {@link #toExternalValue()} method is
   * called. Otherwise, an exception is thrown.
   *
   * @param collection The collection to get the external value from
   * @return A Spark SQL column representing the external value of the collection
   * @throws UnsupportedOperationException If the collection does not implement
   * {@link Materializable}
   */
  @Nonnull
  static Column getExternalValue(@Nonnull final Collection collection) {
    if (collection instanceof final Materializable external) {
      return external.toExternalValue();
    } else {
      throw new UnsupportedOperationException(
          "Cannot obtain value for non-primitive collection of FHIR type: "
              + collection.getFhirType().map(Objects::toString).orElse("unknown"));
    }
  }
}
