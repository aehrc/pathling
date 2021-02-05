/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.QueryHelpers.createColumns;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.QueryHelpers.DatasetWithColumnMap;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Represents a path that is a collection of resources of more than one type.
 *
 * @author John Grimes
 */
public class UntypedResourcePath extends NonLiteralPath implements Referrer {

  /**
   * A column within the dataset containing the resource type.
   */
  @Nonnull
  @Getter
  private final Column typeColumn;

  private UntypedResourcePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final Column typeColumn) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, Optional.empty(),
        thisColumn);

    checkArgument(Arrays.asList(dataset.columns()).contains(typeColumn.toString()),
        "Type column not present in dataset");
    this.typeColumn = typeColumn;
  }

  /**
   * @param referencePath a {@link ReferencePath} to base the new UntypedResourcePath on
   * @param expression the FHIRPath representation of this path
   * @param dataset a {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn a column within the dataset containing the identity of the subject resource
   * @param eidColumn a column within the dataset containing the element identities of the nodes
   * @param typeColumn a column within the dataset containing the resource type
   * @return a shiny new UntypedResourcePath
   */
  @Nonnull
  public static UntypedResourcePath build(@Nonnull final ReferencePath referencePath,
      @Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column typeColumn) {

    final Column valueColumn = referencePath.getValueColumn();
    final DatasetWithColumn datasetWithType = createColumn(dataset, typeColumn);
    final Dataset<Row> finalDataset = datasetWithType.getDataset();
    final Column finalTypeColumn = datasetWithType.getColumn();

    return new UntypedResourcePath(expression, finalDataset, idColumn, eidColumn, valueColumn,
        referencePath.isSingular(), referencePath.getThisColumn(), finalTypeColumn
    );
  }

  @Nonnull
  public Column getReferenceColumn() {
    return valueColumn.getField(Referrer.REFERENCE_FIELD_NAME);
  }

  @Nonnull
  public Column getResourceEquality(@Nonnull final ResourcePath resourcePath) {
    return Referrer.resourceEqualityFor(this, resourcePath);
  }

  @Nonnull
  public Column getResourceEquality(@Nonnull final Column targetId,
      @Nonnull final Column targetCode) {
    return Referrer.resourceEqualityFor(this, targetCode, targetId);
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return Optional.empty();
  }

  @Nonnull
  @Override
  public UntypedResourcePath copy(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Optional<Column> eidColumn, @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<Column> thisColumn) {

    final DatasetWithColumnMap datasetWithColumns = eidColumn.map(eidCol -> createColumns(dataset,
        eidCol, valueColumn)).orElseGet(() -> createColumns(dataset, valueColumn));

    return new UntypedResourcePath(expression, datasetWithColumns.getDataset(), idColumn,
        eidColumn.map(datasetWithColumns::getColumn),
        datasetWithColumns.getColumn(valueColumn), singular, thisColumn, typeColumn);
  }

  @Override
  @Nonnull
  public NonLiteralPath mergeWith(@Nonnull final FhirPath target,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    if (target instanceof UntypedResourcePath) {
      return copy(expression, dataset, idColumn, eidColumn, valueColumn, singular, thisColumn);
    }
    // Anything else is invalid.
    throw new InvalidUserInputError(
        "Paths cannot be merged into a collection together: " + getExpression() + ", " + target
            .getExpression());
  }

}
