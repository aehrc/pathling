package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import java.util.Objects;

/**
 * Interface for collections that can be converted to external values (e.g.: for SQL on FHIR view
 * results).
 */
public interface External {

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
   * If the collection implements {@link External}, its {@link #toExternalValue()} method is called.
   * Otherwise, an exception is thrown.
   *
   * @param collection The collection to get the external value from
   * @return A Spark SQL column representing the external value of the collection
   * @throws UnsupportedOperationException If the collection does not implement {@link External}
   */
  @Nonnull
  static Column getExternalValue(@Nonnull final Collection collection) {
    if (collection instanceof External external) {
      return external.toExternalValue();
    } else {
      throw new UnsupportedOperationException(
          "Cannot obtain value for non-primitive collection of FHIR type: "
              + collection.getFhirType().map(Objects::toString).orElse("unknown"));
    }
  }
}
