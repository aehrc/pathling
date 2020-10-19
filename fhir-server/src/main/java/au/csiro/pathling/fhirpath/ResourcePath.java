/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.convertRawResource;

import au.csiro.pathling.QueryHelpers.DatasetWithIdsAndValue;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.Optional;
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

  /**
   * @param expression the FHIRPath representation of this path
   * @param dataset a {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn a {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param valueColumn a {@link Column} within the dataset containing the values of the nodes
   * @param singular an indicator of whether this path represents a single-valued collection to each
   * FHIRPath type
   * @param thisColumn collection values where this path originated from {@code $this}
   * @param definition the {@link ResourceDefinition} that will be used for subsequent path
   */
  public ResourcePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<Column> thisColumn,
      @Nonnull final ResourceDefinition definition) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, Optional.empty(),
        thisColumn);
    this.definition = definition;
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
    final Dataset<Row> rawDataset = resourceReader.read(resourceType);
    final DatasetWithIdsAndValue dataset = convertRawResource(rawDataset);
    return new ResourcePath(expression, dataset.getDataset(), Optional.of(dataset.getIdColumn()),
        Optional.of(dataset.getEidColumn()),
        dataset.getValueColumn(), singular, Optional.empty(), definition);
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
      @Nonnull final Optional<Column> idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<Column> thisColumn) {
    return new ResourcePath(expression, dataset, idColumn, eidColumn, valueColumn, singular,
        thisColumn,
        definition);
  }

}
