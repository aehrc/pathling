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

import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.encoders.ExtensionSupport;
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
public class ResourceCollection extends Collection {

  /**
   * A mapping between the names of elements in the resource and the corresponding {@link Column}.
   */
  @Nonnull
  private final Map<String, Column> elementsToColumns;

  @Nonnull
  private final ResourceDefinition resourceDefinition;

  public ResourceCollection(@Nonnull final Column column, @Nonnull final ResourceDefinition definition,
      @Nonnull final Map<String, Column> elementsToColumns) {
    super(column, Optional.empty(), getFhirType(definition.getResourceType()),
        Optional.of(definition));
    this.elementsToColumns = elementsToColumns;
    this.resourceDefinition = definition;
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
   * @param dataSource the {@link DataSource} to use for retrieving the Dataset
   * @param resourceType the type of the resource
   * @return A shiny new ResourcePath
   */
  @Nonnull
  public static ResourceCollection build(@Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource, @Nonnull final ResourceType resourceType) {

    // Get the resource definition from HAPI.
    final String resourceCode = resourceType.toCode();
    final RuntimeResourceDefinition hapiDefinition = fhirContext.getResourceDefinition(
        resourceCode);
    final ResourceDefinition definition = new ResourceDefinition(resourceType, hapiDefinition);

    // Retrieve the dataset for the resource type using the supplied resource reader.
    final Dataset<Row> dataset = dataSource.read(resourceType);

    //noinspection ReturnOfNull
    final Map<String, Column> elementsToColumns = Stream.of(dataset.columns())
        .collect(Collectors.toMap(Function.identity(), functions::col, (a, b) -> null));

    // We use the ID column as the value column for a ResourcePath.
    return new ResourceCollection(functions.col("id"), definition, elementsToColumns);
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
  public Column getExtensionContainerColumn() {
    final Optional<Column> maybeExtensionColumn = Optional
        .ofNullable(elementsToColumns.get(ExtensionSupport.EXTENSIONS_FIELD_NAME()));
    return checkPresent(maybeExtensionColumn,
        "Extension container column '_extension' not present in the resource."
            + " Check if extension support was enabled when data were imported!");
  }

  public ResourceType getResourceType() {
    return resourceDefinition.getResourceType();
  }

  @Nonnull
  @Override
  public Optional<Collection> traverse(@Nonnull final String expression) {
    // Get the child column from the map of elements to columns.
    return getElementColumn(expression).flatMap(value ->
        // Get the child element definition from the resource definition.
        resourceDefinition.getChildElement(expression).map(definition ->
            Collection.build(value, definition)));
  }

}
