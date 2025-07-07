package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Used when a collection does not have a representation that can be expressed within a Spark
 * dataframe. This class will throw an error if any attempt is made to access its value or
 * traverse its fields. It accepts a description of the unsupported representation to provide
 * more context in the error message.
 * 
 * @author John Grimes
 */
@AllArgsConstructor
public class UnsupportedRepresentation extends ColumnRepresentation {
  
  /**
   * A description of the unsupported representation, used for error messages.
   */
  @Nonnull
  private final String description;

  @Override
  public Column getValue() {
    throw new UnsupportedFhirPathFeatureError(
        "Representation of this path is not supported: " + description);
  }

  @Override
  protected ColumnRepresentation copyOf(@Nonnull final Column newValue) {
    return new UnsupportedRepresentation(description);
  }

  @Override
  public @Nonnull ColumnRepresentation vectorize(
      @Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return new UnsupportedRepresentation(description);
  }

  @Override
  public @Nonnull ColumnRepresentation flatten() {
    return new UnsupportedRepresentation(description);
  }

  @Override
  public @Nonnull ColumnRepresentation traverse(@Nonnull final String fieldName) {
    throw new UnsupportedFhirPathFeatureError(
        "Traversal is not supported for this path: " + description);
  }

  @Override
  public @Nonnull ColumnRepresentation getField(@Nonnull final String fieldName) {
    throw new UnsupportedFhirPathFeatureError(
        "Field access is not supported for this path: " + description);
  }

  @Override
  public @Nonnull ColumnRepresentation traverse(@Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType) {
    return traverse(fieldName);
  }
  
}
