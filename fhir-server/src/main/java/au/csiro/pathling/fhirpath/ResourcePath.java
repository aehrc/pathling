/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.resourceToIdAndValue;

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
   * @param expression The FHIRPath representation of this path
   * @param dataset A {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn A {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param valueColumn A {@link Column} within the dataset containing the values of the nodes
   * @param singular An indicator of whether this path represents a single-valued collection to each
   * FHIRPath type
   * @param definition The {@link ResourceDefinition} that will be used for subsequent path
   * traversals
   */
  public ResourcePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final ResourceDefinition definition) {
    super(expression, dataset, idColumn, valueColumn, singular);
    this.definition = definition;
  }

  /**
   * Build a new ResourcePath using the supplied {@link FhirContext} and {@link ResourceReader}.
   *
   * @param fhirContext The {@link FhirContext} to use for sourcing the resource definition
   * @param resourceReader The {@link ResourceReader} to use for retrieving the Dataset
   * @param resourceType The type of the resource
   * @param expression The expression to use in the resulting path
   * @param singular Whether the resulting path should be flagged as a single item collection
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
    final Dataset<Row> dataset = resourceToIdAndValue(rawDataset);
    return new ResourcePath(expression, dataset, dataset.col("id"), dataset.col("value"), singular,
        definition);
  }

  public ResourceType getResourceType() {
    return definition.getResourceType();
  }

  @Override
  @Nonnull
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return definition.getChildElement(name);
  }

  @Nonnull
  @Override
  public Optional<Column> getOriginColumn() {
    return Optional.of(getValueColumn());
  }

  @Nonnull
  @Override
  public Optional<ResourceDefinition> getOriginType() {
    return Optional.of(definition);
  }
  
}
