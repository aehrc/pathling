package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.definition.ChoiceElementDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Represents a choice element, which can be resolved to any one of a number of data types.
 *
 * @author John Grimes
 */
public class MixedCollection extends Collection {

  @Nonnull
  private final Collection from;

  @Nonnull
  private final ChoiceElementDefinition choiceDefinition;

  public MixedCollection(@Nonnull final Column column,
      @Nonnull final Optional<ChoiceElementDefinition> definition, @Nonnull final Collection from) {
    super(column, Optional.empty(), Optional.empty(), definition);
    this.from = from;
    this.choiceDefinition = checkPresent(definition);
  }

  @Nonnull
  public static MixedCollection build(@Nonnull final Column column,
      @Nonnull final Optional<ChoiceElementDefinition> definition, @Nonnull final Collection from) {
    return new MixedCollection(column, definition, from);
  }

  @Nonnull
  @Override
  public Optional<Collection> traverse(@Nonnull final String expression) {
    return Optional.empty();
  }

  @Nonnull
  private Optional<ElementDefinition> resolveChoiceDefinition(@Nonnull final String type) {
    return choiceDefinition.getChildByType(type);
  }

  @Nonnull
  public Optional<Collection> resolveChoice(@Nonnull final String type) {
    final String elementName = choiceDefinition.getElementName();
    final String columnName = ChoiceElementDefinition.getColumnName(elementName, type);
    if (from instanceof ResourceCollection) {
      final Optional<Column> elementColumn = ((ResourceCollection) from).getElementColumn(columnName);
      final Optional<ElementDefinition> definition = resolveChoiceDefinition(type);
      checkUserInput(elementColumn.isPresent() && definition.isPresent(),
          "No such child: " + columnName);
      return elementColumn.map(column -> Collection.build(column, definition.get()));
    }
    return from.traverse(columnName);
  }

}
