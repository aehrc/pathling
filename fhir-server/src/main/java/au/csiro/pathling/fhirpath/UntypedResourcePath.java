/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.TYPE_COLUMN_SUFFIX;
import static au.csiro.pathling.utilities.Strings.randomShortString;

import au.csiro.pathling.fhirpath.element.ElementDefinition;
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
   * A {@link Column} within the dataset containing the resource type.
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

  protected UntypedResourcePath(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Optional<Column> idColumn,
      @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final Column typeColumn,
      @Nonnull final Set<ResourceType> possibleTypes) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, Optional.empty(),
        thisColumn);
    this.typeColumn = typeColumn;
    this.possibleTypes = possibleTypes;
  }

  /**
   * @param expression The FHIRPath representation of this path
   * @param dataset A {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn A {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param eidColumn A {@link Column} within the dataset containing the element identies of the
   * nodes
   * @param valueColumn A {@link Column} within the dataset containing the values of the nodes
   * @param singular An indicator of whether this path represents a single-valued collection
   * @param thisColumn collection values where this path originated from {@code $this}
   * @param typeColumn A {@link Column} within the dataset containing the resource type
   * @param possibleTypes A set of {@link ResourceType} objects that describe the different types
   * @return a shiny new UntypedResourcePath
   */
  public static UntypedResourcePath build(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Optional<Column> idColumn,
      @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final Column typeColumn,
      @Nonnull final Set<ResourceType> possibleTypes) {

    final String hash = randomShortString();
    final String typeColumnName = hash + TYPE_COLUMN_SUFFIX;

    final Dataset<Row> hashedDataset = dataset.withColumn(typeColumnName, typeColumn);

    return new UntypedResourcePath(expression, hashedDataset, idColumn, eidColumn, valueColumn,
        singular,
        thisColumn, hashedDataset.col(typeColumnName), possibleTypes);
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return Optional.empty();
  }

  @Nonnull
  @Override
  public UntypedResourcePath copy(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Optional<Column> idColumn,
      @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    return new UntypedResourcePath(expression, dataset, idColumn, eidColumn, valueColumn, singular,
        thisColumn,
        typeColumn, possibleTypes);
  }

}
