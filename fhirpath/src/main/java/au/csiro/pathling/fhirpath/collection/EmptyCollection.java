package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents an empty collection.
 */
public class EmptyCollection extends Collection implements Comparable, Numeric, StringCoercible,
    Materializable {

  private static final EmptyCollection INSTANCE = new EmptyCollection(
      DefaultRepresentation.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
      Optional.empty());

  protected EmptyCollection(@Nonnull final ColumnRepresentation column,
      @Nonnull final Optional<FhirPathType> type, @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(column, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * @return A singleton instance of this class
   */
  public static EmptyCollection getInstance() {
    return INSTANCE;
  }

  @Override
  @Nonnull
  public Collection copyWith(@Nonnull final ColumnRepresentation column) {
    return this;
  }

  @Override
  public boolean isComparableTo(@Nonnull final Comparable path) {
    return true;
  }

  @Override
  public boolean convertibleTo(@Nonnull final Collection other) {
    return true;
  }

  @Override
  @Nonnull
  public StringCollection asStringPath() {
    return StringCollection.empty();
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation returns an empty BooleanCollection
   */
  @Override
  @Nonnull
  public BooleanCollection asBooleanPath() {
    return BooleanCollection.empty();
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation returns an empty BooleanCollection
   */
  @Override
  @Nonnull
  public BooleanCollection asBooleanSingleton() {
    return BooleanCollection.empty();
  }


  @Override
  public @Nonnull Function<Numeric, Collection> getMathOperation(
      @Nonnull final Numeric.MathOperation operation) {
    return numeric -> {
      // For empty collections, all math operations return an empty collection
      return this;
    };
  }

  @Override
  public @Nonnull Collection negate() {
    return this;
  }
}
