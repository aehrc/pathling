package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.definition.ChoiceElementDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a choice element, which can be resolved to any one of a number of data types.
 *
 * @author John Grimes
 */
@Getter
public class MixedCollection extends Collection {

  /**
   * The collection from which this choice element is derived.
   */
  @Nonnull
  private final Collection from;

  /**
   * The definition of this choice element.
   */
  @Nonnull
  private final ChoiceElementDefinition choiceDefinition;

  protected MixedCollection(@Nonnull final Column column,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      final boolean singular, @Nonnull final Collection from) {
    super(column, type, fhirType, definition);
    this.from = from;
    checkArgument(definition.isPresent() && definition.get() instanceof ChoiceElementDefinition,
        "definition must be a ChoiceElementDefinition");
    this.choiceDefinition = (ChoiceElementDefinition) definition.get();
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param column The column to use
   * @param definition The definition to use
   * @param singular Whether the collection is singular
   * @param from The collection from which this choice element is derived
   * @return A new instance of {@link MixedCollection}
   */
  @Nonnull
  public static MixedCollection build(@Nonnull final Column column,
      @Nonnull final Optional<ChoiceElementDefinition> definition, final boolean singular,
      @Nonnull final Collection from) {
    return new MixedCollection(column, Optional.empty(), Optional.empty(), definition, singular,
        from);
  }

  @Nonnull
  @Override
  public Optional<Collection> traverse(@Nonnull final String elementName) {
    return Optional.empty();
  }

  @Nonnull
  private Optional<ElementDefinition> resolveChoiceDefinition(@Nonnull final String type) {
    return choiceDefinition.getChildByType(type);
  }

  /**
   * Returns a new collection representing just the elements of this collection with the specified
   * type.
   *
   * @param type The type of element to return
   * @param context The {@link EvaluationContext} to use
   * @return A new collection representing just the elements of this collection with the specified
   * type
   */
  @Nonnull
  public Optional<Collection> resolveChoice(@Nonnull final String type,
      @Nonnull final EvaluationContext context) {
    final String elementName = choiceDefinition.getElementName();
    final String columnName = ChoiceElementDefinition.getColumnName(elementName, type);
    if (from instanceof ResourceCollection) {
      final Optional<Column> elementColumn = ((ResourceCollection) from).getElementColumn(
          columnName);
      final Optional<ElementDefinition> definition = resolveChoiceDefinition(type);
      checkUserInput(elementColumn.isPresent() && definition.isPresent(),
          "No such child: " + columnName);
      return elementColumn.map(column -> Collection.build(column, definition.get()));
    }
    return from.traverse(columnName);
  }
}
