/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.aliasColumns;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.DatasetWithColumnMap;
import au.csiro.pathling.QueryHelpers.DatasetWithColumns;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents any FHIRPath expression which refers to a resource type.
 *
 * @author John Grimes
 */
public class ResourcePath extends NonLiteralPath {

  @Nonnull
  @Getter
  private final ResourceDefinition definition;

  @Nonnull
  private final Map<String, Column> elementsToColumns;

  protected ResourcePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final List<Column> valueColumns,
      final boolean singular, @Nonnull final Optional<List<Column>> thisColumns,
      @Nonnull final ResourceDefinition definition,
      @Nonnull final Map<String, Column> elementsToColumns) {
    super(expression, dataset, idColumn, valueColumns, singular, Optional.empty(), thisColumns);
    this.definition = definition;
    this.elementsToColumns = elementsToColumns;
  }

  /**
   * Build a new ResourcePath using the supplied {@link FhirContext} and {@link ResourceReader}.
   *
   * @param fhirContext the {@link FhirContext} to use for sourcing the resource definition
   * @param resourceReader the {@link ResourceReader} to use for retrieving the Dataset
   * @param resourceType the type of the resource
   * @param expression the expression to use in the resulting path
   * @param singular whether the resulting path should be flagged as a single item collection
   * @return A shiny new ResourcePath
   */
  @Nonnull
  public static ResourcePath build(@Nonnull final FhirContext fhirContext,
      @Nonnull final ResourceReader resourceReader, @Nonnull final ResourceType resourceType,
      @Nonnull final String expression, final boolean singular) {
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);
    final Dataset<Row> dataset = resourceReader.read(resourceType);

    final Column idColumn = dataset.col("id");
    final List<Column> valueColumns = Arrays.stream(dataset.columns())
        .map(dataset::col)
        .collect(Collectors.toList());

    final DatasetWithColumnMap datasetWithColumnMap = aliasColumns(dataset, valueColumns, false);
    final Dataset<Row> aliasedDataset = datasetWithColumnMap.getDataset();
    final Optional<Column> aliasedIdColumn = Optional
        .of(datasetWithColumnMap.getColumnMap().get(idColumn));
    final List<Column> aliasedValueColumns = datasetWithColumnMap.getMappedColumns(valueColumns);
    final Map<String, Column> elementsToColumns = new HashMap<>();

    // Build an element to column map, which can be used to retrieve columns by element name when
    // performing path traversal.
    datasetWithColumnMap.getColumnMap()
        .forEach((source, target) -> elementsToColumns.put(source.toString(), target));

    return new ResourcePath(expression, aliasedDataset, aliasedIdColumn, aliasedValueColumns,
        singular, Optional.empty(), definition, elementsToColumns);
  }

  /**
   * Build a new ResourcePath using the supplied {@link FhirContext} and {@link ResourceReader}.
   *
   * @param fhirContext the {@link FhirContext} to use for sourcing the resource definition
   * @param resourceReader the {@link ResourceReader} to use for retrieving the Dataset
   * @param resourceType the type of the resource
   * @param expression the expression to use in the resulting path
   * @param singular whether the resulting path should be flagged as a single item collection
   * @param skipAliasing set to true to skip column aliasing
   * @return A shiny new ResourcePath
   */
  @SuppressWarnings("DuplicatedCode")
  @Nonnull
  public static ResourcePath build(@Nonnull final FhirContext fhirContext,
      @Nonnull final ResourceReader resourceReader, @Nonnull final ResourceType resourceType,
      @Nonnull final String expression, final boolean singular, final boolean skipAliasing) {
    if (!skipAliasing) {
      return build(fhirContext, resourceReader, resourceType, expression, singular);
    }
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);
    final Dataset<Row> dataset = resourceReader.read(resourceType);

    final Column idColumn = dataset.col("id");
    final List<Column> valueColumns = Arrays.stream(dataset.columns())
        .map(dataset::col)
        .collect(Collectors.toList());
    final Map<String, Column> elementsToColumns = new HashMap<>();
    Stream.of(dataset.columns())
        .forEach(columnName -> elementsToColumns.put(columnName, dataset.col(columnName)));

    return new ResourcePath(expression, dataset, Optional.of(idColumn), valueColumns, singular,
        Optional.empty(), definition, elementsToColumns);
  }

  @Nonnull
  private static ResourcePath build(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Optional<Column> idColumn,
      @Nonnull final List<Column> valueColumns, final boolean singular,
      @Nonnull final Optional<List<Column>> thisColumns,
      @Nonnull final ResourceDefinition definition,
      @Nonnull final Map<String, Column> elementsToColumns) {
    return new ResourcePath(expression, dataset, idColumn, valueColumns, singular,
        thisColumns, definition, elementsToColumns);
  }

  @Nonnull
  @Override
  protected DatasetWithColumns initializeDatasetAndValues(@Nonnull final Dataset<Row> dataset,
      @Nonnull final List<Column> valueColumns) {
    // We override the default dataset initialisation within NonLiteralPath - replacing it with
    // context-aware initialisation in the builder methods.
    return new DatasetWithColumns(dataset, valueColumns);
  }

  /**
   * @param elementName the name of the element
   * @return the {@link Column} within the dataset pertaining to this element
   */
  public Column getElementColumn(@Nonnull final String elementName) {
    checkUserInput(elementsToColumns.containsKey(elementName),
        "Requested element not present in source data: " + elementName);
    return elementsToColumns.get(elementName);
  }

  public ResourceType getResourceType() {
    return definition.getResourceType();
  }

  @Override
  @Nonnull
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return definition.getChildElement(name);
  }

  public void setForeignResource(@Nonnull final ResourcePath foreignResource) {
    this.foreignResource = Optional.of(foreignResource);
  }

  @Nonnull
  @Override
  public ResourcePath copy(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final List<Column> valueColumns,
      final boolean singular, @Nonnull final Optional<List<Column>> thisColumns) {
    return build(expression, dataset, idColumn, valueColumns, singular, thisColumns,
        definition, elementsToColumns);
  }

}
