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

package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.column.SingleRowCtx;
import au.csiro.pathling.fhirpath.column.StdColumnCtx;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import scala.collection.JavaConverters;

/**
 * Represents any FHIRPath expression which refers to a resource type.
 *
 * @author John Grimes
 */
@Getter
public class ResourceCollection extends Collection {

  /**
   * A mapping between the names of elements in the resource and the corresponding {@link Column}.
   */
  @Nonnull
  private final Map<String, Column> elementsToColumns;

  /**
   * The {@link ResourceDefinition} for this resource type.
   */
  @Nonnull
  private final ResourceDefinition resourceDefinition;

  /**
   * The {@link Dataset} containing the resource data.
   */
  @Nonnull
  private final Dataset<Row> dataset;

  protected ResourceCollection(@Nonnull final Column column,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Map<String, Column> elementsToColumns,
      @Nonnull final ResourceDefinition resourceDefinition,
      @Nonnull final Dataset<Row> dataset) {
    super(column, type, fhirType, definition);
    this.elementsToColumns = elementsToColumns;
    this.resourceDefinition = resourceDefinition;
    this.dataset = dataset;
  }

  @Nonnull
  private static Optional<FHIRDefinedType> getFhirType(@Nonnull final ResourceType resourceType) {
    try {
      return Optional.ofNullable(FHIRDefinedType.fromCode(resourceType.toCode()));
    } catch (final FHIRException e) {
      return Optional.empty();
    }
  }

  /**
   * Build a new ResourcePath using the supplied {@link FhirContext} and {@link DataSource}.
   *
   * @param fhirContext the {@link FhirContext} to use for sourcing the resource definition
   * @param dataset the {@link Dataset} that contains the resource data
   * @param resourceType the type of the resource
   * @return A shiny new ResourcePath
   */
  @Nonnull
  public static ResourceCollection build(@Nonnull final FhirContext fhirContext,
      @Nonnull final Dataset<Row> dataset, @Nonnull final ResourceType resourceType) {

    // Get the resource definition from HAPI.
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext.getResourceDefinition(
        resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);

    //noinspection ReturnOfNull
    final Map<String, Column> elementsToColumns = Stream.of(dataset.columns())
        .collect(Collectors.toUnmodifiableMap(Function.identity(), dataset::col));

    // We use a literal column as the resource value - the actual value is not important.
    // But the non-null value indicates that the resource should be included in any result.
    return new ResourceCollection(functions.lit(true), Optional.empty(),
        getFhirType(resourceType), Optional.of(definition), elementsToColumns, definition,
        dataset);
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

  /**
   * @param elementName the name of the element
   * @return the {@link Column} within the dataset pertaining to this element
   */
  @Nonnull
  public Optional<Column> getElementColumn(@Nonnull final String elementName) {
    return Optional.ofNullable(elementsToColumns.get(elementName));
  }

  @Nonnull
  @Override
  protected Column getFid() {
    return getElementColumn(ExtensionSupport.FID_FIELD_NAME()).orElseThrow(
        () -> new IllegalStateException("Resource does not have an 'id' column"));
  }

  /**
   * @return the {@link ResourceType} of this resource collection
   */
  public ResourceType getResourceType() {
    return resourceDefinition.getResourceType();
  }


  @Nonnull
  @Override
  protected Collection traverseElement(@Nonnull final ElementDefinition childDef) {
    // TODO: what does mean if an element is present in the definition but not in 
    // the schema?
    return getElementColumn(childDef.getElementName()).map(
        value -> Collection.build(
            // TODO: simplify this
            functions.when(getCtx().getValue().isNotNull(), value),
            childDef)).get();
  }


  @Nonnull
  @Override
  public Collection copyWith(@Nonnull final Column newValue) {
    return new ResourceCollection(newValue, getType(), getFhirType(), getDefinition(),
        elementsToColumns, resourceDefinition, dataset);
  }

  @Nonnull
  public StdColumnCtx getKeyColumn() {
    return getElementColumn("id_versioned")
        .map(StdColumnCtx::of)
        .orElseThrow(
            () -> new IllegalStateException("Resource does not have an 'id_versioned' column"));
  }

  @Nonnull
  @Override
  public ColumnCtx getCtx() {
    return SingleRowCtx.of(getColumn());
  }
}