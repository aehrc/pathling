/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a path that is a collection of resources of more than one type.
 *
 * @author John Grimes
 */
public class UntypedResourcePath extends NonLiteralPath {

  /**
   * A column within the dataset containing the resource type.
   */
  @Nonnull
  @Getter
  private final Column typeColumn;

  /**
   * A set of {@link ResourceType} objects that describe the different types that this collection
   * may contain.
   */
  @Nonnull
  @Getter
  private final Set<ResourceType> possibleTypes;

  private UntypedResourcePath(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final Column typeColumn,
      @Nonnull final Set<ResourceType> possibleTypes) {
    super(expression, dataset, idColumn, valueColumn, singular, Optional.empty(), thisColumn);

    checkArgument(Arrays.asList(dataset.columns()).contains(typeColumn.toString()),
        "Type column not present in dataset");

    this.typeColumn = typeColumn;
    this.possibleTypes = possibleTypes;
  }

  /**
   * @param referencePath a {@link ReferencePath} to base the new UntypedResourcePath on
   * @param expression the FHIRPath representation of this path
   * @param dataset a {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn a column within the dataset containing the identity of the subject resource
   * @param typeColumn a column within the dataset containing the resource type
   * @param possibleTypes a set of {@link ResourceType} objects that describe the different types
   * @return a shiny new UntypedResourcePath
   */
  @Nonnull
  public static UntypedResourcePath build(@Nonnull final ReferencePath referencePath,
      @Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column typeColumn,
      @Nonnull final Set<ResourceType> possibleTypes) {

    final Column valueColumn = referencePath.getValueColumn();
    final DatasetWithColumn datasetWithType = createColumn(dataset, typeColumn);
    final Dataset<Row> finalDataset = datasetWithType.getDataset();
    final Column finalTypeColumn = datasetWithType.getColumn();

    return new UntypedResourcePath(expression, finalDataset, idColumn, valueColumn,
        referencePath.isSingular(), referencePath.getThisColumn(), finalTypeColumn,
        possibleTypes);
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
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    final DatasetWithColumn datasetWithColumn = createColumn(dataset, valueColumn);
    return new UntypedResourcePath(expression, datasetWithColumn.getDataset(), idColumn,
        datasetWithColumn.getColumn(), singular, thisColumn, typeColumn, possibleTypes);
  }

}
