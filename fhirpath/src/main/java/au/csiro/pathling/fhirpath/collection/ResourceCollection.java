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
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.fhirpath.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.spark.sql.Column;
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
   * The {@link ResourceDefinition} for this resource type.
   */
  @Nonnull
  private final ResourceDefinition resourceDefinition;

  protected ResourceCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final ResourceDefinition resourceDefinition) {
    super(columnRepresentation, type, fhirType, definition,
        Optional.of(
            columnRepresentation.traverse(ExtensionSupport.EXTENSIONS_FIELD_NAME()).getValue()));
    this.resourceDefinition = resourceDefinition;
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
   * Build a new ResourcePath using the supplied {@link FhirContext} and {@link ResourceType}.
   *
   * @param fhirContext the {@link FhirContext} to use for sourcing the resource definition
   * @param resourceType the type of the resource
   * @return A shiny new ResourcePath
   */
  @Nonnull
  public static ResourceCollection build(@Nonnull final FhirContext fhirContext,
      @Nonnull final ResourceType resourceType) {

    final ResourceDefinition definition = FhirDefinitionContext.of(fhirContext)
        .findResourceDefinition(resourceType);

    // We use a literal column as the resource value - the actual value is not important.
    // But the non-null value indicates that the resource should be included in any result.
    return new ResourceCollection(new DefaultRepresentation(functions.lit(true)),
        Optional.empty(),
        getFhirType(resourceType), Optional.of(definition), definition);
  }

  /**
   * Build a new ResourcePath using the supplied {@link ColumnRepresentation}, {@link FhirContext},
   * and {@link ResourceType}.
   *
   * @param columnRepresentation A column representation to use for the resource
   * @param fhirContext The {@link FhirContext} to use for sourcing the resource definition
   * @param resourceType The type of the resource
   * @return A shiny new ResourcePath
   */
  @Nonnull
  public static ResourceCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ResourceType resourceType) {
    // Get the resource definition from HAPI.
    final ResourceDefinition definition = FhirDefinitionContext.of(fhirContext)
        .findResourceDefinition(resourceType);

    // We use a literal column as the resource value - the actual value is not important.
    // But the non-null value indicates that the resource should be included in any result.
    return new ResourceCollection(columnRepresentation, Optional.empty(),
        getFhirType(resourceType), Optional.of(definition), definition);
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
  public Optional<ColumnRepresentation> getElementColumn(@Nonnull final String elementName) {
    return Optional.of(functions.col(elementName))
        .map(DefaultRepresentation::new);
  }


  @Nonnull
  @Override
  protected ColumnRepresentation getFid() {
    // return getElementColumn(ExtensionSupport.FID_FIELD_NAME()).orElseThrow(
    //     () -> new IllegalStateException("Resource does not have an 'id' column"));
    return getColumn().traverse(ExtensionSupport.FID_FIELD_NAME());
  }

  /**
   * @return the {@link ResourceType} of this resource collection
   */
  public ResourceType getResourceType() {
    return resourceDefinition.getResourceType();
  }

  @Nonnull
  @Override
  public Collection copyWith(@Nonnull final ColumnRepresentation newValue) {
    return new ResourceCollection(newValue, getType(), getFhirType(), getDefinition(),
        resourceDefinition);
  }

  /**
   * @return A column that can be used as a key for joining to this resource type
   */
  @Nonnull
  public Collection getKeyCollection() {
    return StringCollection.build(
        getColumn().traverse("id_versioned", Optional.of(FHIRDefinedType.STRING))
    );
  }

}
