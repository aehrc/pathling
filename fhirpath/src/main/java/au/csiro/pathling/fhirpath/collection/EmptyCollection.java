package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.EmptyRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents an empty collection.
 */
public class EmptyCollection extends Collection {

  private static final EmptyCollection INSTANCE = new EmptyCollection(
      EmptyRepresentation.getInstance(), Optional.empty(), Optional.empty(), Optional.empty());

  protected EmptyCollection(@Nonnull final ColumnRepresentation column,
      @Nonnull final Optional<FhirPathType> type, @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    super(column, type, fhirType, definition);
  }

  /**
   * @return A singleton instance of this class
   */
  public static EmptyCollection getInstance() {
    return INSTANCE;
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return true;
  }
 
}
