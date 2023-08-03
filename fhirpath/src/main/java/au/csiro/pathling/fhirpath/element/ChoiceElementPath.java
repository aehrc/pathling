package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.AbstractPath;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.definition.ChoiceElementDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.operator.PathTraversalOperator;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ChoiceElementPath extends NonLiteralPath implements AbstractPath {

  @Getter(AccessLevel.PUBLIC)
  @Nonnull
  protected final ChoiceElementDefinition definition;

  @Getter
  @Nonnull
  private final Optional<Column> orderingColumn;

  @Nonnull
  private final NonLiteralPath from;

  protected ChoiceElementPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn,
      @Nonnull final Optional<Column> orderingColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn,
      @Nonnull final ChoiceElementDefinition definition, @Nonnull final NonLiteralPath from) {
    super(expression, dataset, idColumn, valueColumn, singular, currentResource, thisColumn);
    this.orderingColumn = orderingColumn;
    this.definition = definition;
    this.from = from;
  }

  @Nonnull
  public static ChoiceElementPath build(@Nonnull final String expression,
      @Nonnull final NonLiteralPath from, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column valueColumn, @Nonnull final Optional<Column> orderingColumn,
      final boolean singular, final ChoiceElementDefinition definition) {
    return new ChoiceElementPath(expression, dataset, from.getIdColumn(), valueColumn,
        orderingColumn, singular, from.getCurrentResource(), from.getThisColumn(), definition,
        from);
  }

  @Nonnull
  @Override
  public NonLiteralPath combineWith(@Nonnull final FhirPath target,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    throw new InvalidUserInputError(
        "Paths cannot be merged into a collection together: " + getExpression() + ", " + target
            .getExpression());
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return Optional.empty();
  }

  @Nonnull
  public Optional<ElementDefinition> resolveChoice(@Nonnull final String type) {
    return definition.getChildByType(type);
  }

  @Nonnull
  public Optional<Column> resolveChoiceColumn(@Nonnull final String type) {
    if (from instanceof ResourcePath) {
      return ((ResourcePath) from).getElementColumn(
          ChoiceElementDefinition.getColumnName(definition.getElementName(), type));
    } else if (from instanceof ElementPath) {
      return Optional.of(PathTraversalOperator.buildTraversalColumn(from, type));
    } else {
      throw new IllegalStateException("Cannot resolve choice column from " + from);
    }
  }

  @Nonnull
  @Override
  public NonLiteralPath copy(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn,
      @Nonnull final Optional<Column> orderingColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    return new ChoiceElementPath(expression, dataset, idColumn, valueColumn, orderingColumn,
        singular, currentResource, thisColumn, definition, from);
  }

}
