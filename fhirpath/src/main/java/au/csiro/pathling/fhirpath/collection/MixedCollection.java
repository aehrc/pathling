package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.definition.ChoiceChildDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a polymorphic collection, which can be resolved to any one of a number of data types.
 *
 * @author John Grimes
 */
@Getter
public class MixedCollection extends Collection {

  /**
   * The definition of this choice element.
   */
  @Nonnull
  private final ChoiceChildDefinition choiceDefinition;

  @Nonnull
  private final Collection parent;

  protected MixedCollection(@Nonnull final Column column,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Collection parent) {
    super(column, type, fhirType, definition);
    checkArgument(definition.isPresent() && definition.get() instanceof ChoiceChildDefinition,
        "definition must be a ChoiceElementDefinition");
    this.choiceDefinition = (ChoiceChildDefinition) definition.get();
    this.parent = parent;
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param definition The definition to use
   * @return A new instance of {@link MixedCollection}
   */
  @Nonnull
  public static MixedCollection build(@Nonnull final Collection parent,
      @Nonnull final ChoiceChildDefinition definition) {
    return new MixedCollection(functions.lit(null), Optional.empty(), Optional.empty(),
        Optional.of(definition), parent);
  }


  @Nonnull
  @Override
  public Optional<Collection> traverse(@Nonnull final String elementName) {
    // FHIRPATH_NOTE: this should technically result in unchecked traversal
    throw new UnsupportedOperationException(
        "Direct traversal of polymorphic collections is not supported."
            + " Please use 'ofType()' to specify the type of element to traverse.");
  }

  /**
   * Returns a new collection representing just the elements of this collection with the specified
   * type.
   *
   * @param type The type of element to return
   * @return A new collection representing just the elements of this collection with the specified
   * type
   */
  @Nonnull
  public Optional<Collection> resolveChoice(@Nonnull final String type) {
    return choiceDefinition.getChildByType(type).map(
        parent::traverseElement
    );
  }
}
