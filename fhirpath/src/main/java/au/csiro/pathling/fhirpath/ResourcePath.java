/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.aliasAllColumns;
import static au.csiro.pathling.QueryHelpers.createColumns;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.col;

import au.csiro.pathling.QueryHelpers.DatasetWithColumnMap;
import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import scala.collection.JavaConverters;

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
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final ResourceDefinition definition,
      @Nonnull final Map<String, Column> elementsToColumns) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, Optional.empty(),
        thisColumn);
    this.definition = definition;
    this.elementsToColumns = elementsToColumns;
    this.setCurrentResource(this);
  }

  /**
   * Build a new ResourcePath using the supplied {@link FhirContext} and {@link DataSource}.
   *
   * @param fhirContext the {@link FhirContext} to use for sourcing the resource definition
   * @param dataSource the {@link DataSource} to use for retrieving the Dataset
   * @param resourceType the type of the resource
   * @param expression the expression to use in the resulting path
   * @param singular whether the resulting path should be flagged as a single item collection
   * @return A shiny new ResourcePath
   */
  @Nonnull
  public static ResourcePath build(@Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource, @Nonnull final ResourceType resourceType,
      @Nonnull final String expression, final boolean singular) {
    return build(fhirContext, dataSource, resourceType, expression, singular, false);
  }

  /**
   * Build a new ResourcePath using the supplied {@link FhirContext} and {@link DataSource}.
   *
   * @param fhirContext the {@link FhirContext} to use for sourcing the resource definition
   * @param dataSource the {@link DataSource} to use for retrieving the Dataset
   * @param resourceType the type of the resource
   * @param expression the expression to use in the resulting path
   * @param singular whether the resulting path should be flagged as a single item collection
   * @param skipAliasing set to true to skip column aliasing
   * @return A shiny new ResourcePath
   */
  @Nonnull
  public static ResourcePath build(@Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource, @Nonnull final ResourceType resourceType,
      @Nonnull final String expression, final boolean singular, final boolean skipAliasing) {

    // Get the resource definition from HAPI.
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext
        .getResourceDefinition(resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);

    // Retrieve the dataset for the resource type using the supplied resource reader.
    final Dataset<Row> dataset = dataSource.read(resourceType);

    final Column idColumn = col("id");
    final Column finalIdColumn;
    final Dataset<Row> finalDataset;
    final Map<String, Column> elementsToColumns;

    if (skipAliasing) {
      // If aliasing is disabled, the dataset will contain columns with the original element names.
      // This is used for contexts where we need the original column names for encoding (e.g.
      // search).
      finalDataset = dataset;
      finalIdColumn = idColumn;
      elementsToColumns = Stream.of(dataset.columns())
          .collect(Collectors.toMap(Function.identity(), functions::col, (a, b) -> null));
    } else {
      // If aliasing is enabled, all columns in the dataset will be aliased, and the original
      // columns will be dropped. This is to avoid column name clashes when doing joins.
      final DatasetWithColumnMap datasetWithColumnMap = aliasAllColumns(dataset);
      finalDataset = datasetWithColumnMap.getDataset();
      final Map<Column, Column> columnMap = datasetWithColumnMap.getColumnMap();
      elementsToColumns = columnMap.keySet().stream()
          .collect(Collectors.toMap(Column::toString, columnMap::get, (a, b) -> null));
      finalIdColumn = elementsToColumns.get(idColumn.toString());
    }

    // We use the ID column as the value column for a ResourcePath.
    return new ResourcePath(expression, finalDataset, finalIdColumn, Optional.empty(),
        finalIdColumn, singular, Optional.empty(), definition, elementsToColumns);
  }

  /**
   * @param elementName the name of the element
   * @return the {@link Column} within the dataset pertaining to this element
   */
  @Nonnull
  public Column getElementColumn(@Nonnull final String elementName) {
    return requireNonNull(elementsToColumns.get(elementName));
  }

  @Nonnull
  @Override
  public Column getExtensionContainerColumn() {
    final Optional<Column> maybeExtensionColumn = Optional
        .ofNullable(elementsToColumns.get(ExtensionSupport.EXTENSIONS_FIELD_NAME()));
    return checkPresent(maybeExtensionColumn,
        "Extension container column '_extension' not present in the resource."
            + " Check if extension support was enabled when data were imported!");
  }

  public ResourceType getResourceType() {
    return definition.getResourceType();
  }

  @Override
  @Nonnull
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return definition.getChildElement(name);
  }

  public void setCurrentResource(@Nonnull final ResourcePath currentResource) {
    this.currentResource = Optional.of(currentResource);
  }

  @Nonnull
  @Override
  public ResourcePath copy(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {

    final DatasetWithColumnMap datasetWithColumns = eidColumn.map(eidCol -> createColumns(dataset,
        eidCol, valueColumn)).orElseGet(() -> createColumns(dataset, valueColumn));

    return new ResourcePath(expression, datasetWithColumns.getDataset(), idColumn,
        eidColumn.map(datasetWithColumns::getColumn),
        datasetWithColumns.getColumn(valueColumn), singular, thisColumn, definition,
        elementsToColumns);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    // A ResourcePath can be combined with another ResourcePath of the same type.
    return super.canBeCombinedWith(target) ||
        (target instanceof ResourcePath &&
            ((ResourcePath) target).getResourceType().equals(getResourceType()));
  }

  @Override
  @Nonnull
  public NonLiteralPath combineWith(@Nonnull final FhirPath target,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    if (target instanceof ResourcePath && definition
        .equals(((ResourcePath) target).getDefinition())) {
      // Two ResourcePaths can be merged together if they have the same definition.
      return copy(expression, dataset, idColumn, eidColumn, valueColumn, singular, thisColumn);
    }
    // Anything else is invalid.
    throw new InvalidUserInputError(
        "Paths cannot be merged into a collection together: " + getExpression() + ", " + target
            .getExpression());
  }

  /**
   * @return The set of resource types currently supported by this implementation.
   */
  @Nonnull
  public static Set<ResourceType> supportedResourceTypes() {
    final Set<ResourceType> availableResourceTypes = EnumSet.allOf(
        ResourceType.class);
    final Set<ResourceType> unsupportedResourceTypes =
        JavaConverters.setAsJavaSet(EncoderBuilder.UNSUPPORTED_RESOURCES()).stream()
            .map(ResourceType::fromCode)
            .collect(Collectors.toSet());
    availableResourceTypes.removeAll(unsupportedResourceTypes);
    availableResourceTypes.remove(ResourceType.RESOURCE);
    availableResourceTypes.remove(ResourceType.DOMAINRESOURCE);
    availableResourceTypes.remove(ResourceType.NULL);
    availableResourceTypes.remove(ResourceType.OPERATIONDEFINITION);
    return availableResourceTypes;
  }

}
