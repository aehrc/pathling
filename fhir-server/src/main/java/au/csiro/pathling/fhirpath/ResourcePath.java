/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.aliasColumns;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.DatasetWithColumnMap;
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
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<Column> thisColumn,
      @Nonnull final ResourceDefinition definition,
      @Nonnull final Map<String, Column> elementsToColumns) {
    super(expression, dataset, idColumn, valueColumn, singular, Optional.empty(), thisColumn);
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
    return build(fhirContext, resourceReader, resourceType, expression, singular, false);
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
  @Nonnull
  public static ResourcePath build(@Nonnull final FhirContext fhirContext,
      @Nonnull final ResourceReader resourceReader, @Nonnull final ResourceType resourceType,
      @Nonnull final String expression, final boolean singular, final boolean skipAliasing) {

    // Get the resource definition from HAPI.
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);

    // Retrieve the dataset for the resource type using the supplied resource reader.
    final Dataset<Row> dataset = resourceReader.read(resourceType);

    final Column idColumn = dataset.col("id");
    final List<Column> allColumns = Arrays.stream(dataset.columns())
        .map(dataset::col)
        .collect(Collectors.toList());

    final Dataset<Row> finalDataset;
    final Column finalIdColumn;
    final Map<String, Column> elementsToColumns = new HashMap<>();

    if (skipAliasing) {
      // If aliasing is disabled, the dataset will contain columns with the original element names.
      // This is used for contexts where we need the original column names for encoding (e.g.
      // search).
      finalDataset = dataset;
      finalIdColumn = idColumn;
      Stream.of(dataset.columns())
          .forEach(columnName -> elementsToColumns.put(columnName, dataset.col(columnName)));
    } else {
      // If aliasing is enabled, all columns in the dataset will be aliased, and the original
      // columns will be dropped. This is to avoid column name clashes when doing joins.
      final DatasetWithColumnMap datasetWithColumnMap = aliasColumns(dataset, allColumns, false);
      finalDataset = datasetWithColumnMap.getDataset();
      finalIdColumn = datasetWithColumnMap.getColumnMap().get(idColumn);
      datasetWithColumnMap.getColumnMap()
          .forEach((source, target) -> elementsToColumns.put(source.toString(), target));
    }

    // We use the ID column as the value column for a ResourcePath.
    return new ResourcePath(expression, finalDataset, finalIdColumn, finalIdColumn, singular,
        Optional.empty(), definition, elementsToColumns);
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
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    return new ResourcePath(expression, dataset, idColumn, valueColumn, singular, thisColumn,
        definition, elementsToColumns);
  }

}
