/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.fhirpath.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Getter;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents any FHIRPath expression which refers to a resource type.
 *
 * @author John Grimes
 */
@Getter
public class ResourceCollection extends Collection {

  /** The {@link ResourceDefinition} for this resource type. */
  @Nonnull private final ResourceDefinition resourceDefinition;

  /**
   * Creates a new ResourceCollection.
   *
   * @param columnRepresentation the column representation
   * @param type the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   * @param resourceDefinition the resource definition
   */
  protected ResourceCollection(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final ResourceDefinition resourceDefinition) {
    super(
        columnRepresentation,
        type,
        fhirType,
        definition,
        Optional.of(
            columnRepresentation.traverse(ExtensionSupport.EXTENSIONS_FIELD_NAME()).getValue()));
    this.resourceDefinition = resourceDefinition;
  }

  @Nonnull
  private static Optional<FHIRDefinedType> getFhirType(@Nonnull final ResourceType resourceType) {
    return getFhirType(resourceType.toCode());
  }

  @Nonnull
  private static Optional<FHIRDefinedType> getFhirType(@Nonnull final String resourceCode) {
    try {
      return Optional.ofNullable(FHIRDefinedType.fromCode(resourceCode));
    } catch (final FHIRException e) {
      return Optional.empty();
    }
  }

  /**
   * Builds a new ResourceCollection from a column representation and resource definition.
   *
   * @param columnRepresentation the column representation
   * @param definition the resource definition
   * @return a new ResourceCollection
   */
  @Nonnull
  public static ResourceCollection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final ResourceDefinition definition) {

    // We use a literal column as the resource value - the actual value is not important.
    // But the non-null value indicates that the resource should be included in any result.
    return new ResourceCollection(
        columnRepresentation,
        Optional.empty(),
        Optional.empty(),
        Optional.of(definition),
        definition);
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
  public static ResourceCollection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ResourceType resourceType) {
    // Get the resource definition from HAPI.
    final ResourceDefinition definition =
        FhirDefinitionContext.of(fhirContext).findResourceDefinition(resourceType);

    // We use a literal column as the resource value - the actual value is not important.
    // But the non-null value indicates that the resource should be included in any result.
    return new ResourceCollection(
        columnRepresentation,
        Optional.empty(),
        getFhirType(resourceType),
        Optional.of(definition),
        definition);
  }

  /**
   * Build a new ResourcePath using the supplied {@link ColumnRepresentation}, {@link FhirContext},
   * and resource code string. This method supports both standard FHIR resource types and custom
   * resource types (like ViewDefinition) that are registered with HAPI but not part of the standard
   * FHIR specification.
   *
   * @param columnRepresentation A column representation to use for the resource
   * @param fhirContext The {@link FhirContext} to use for sourcing the resource definition
   * @param resourceCode The resource type code (e.g., "Patient", "ViewDefinition")
   * @return A shiny new ResourcePath
   */
  @Nonnull
  public static ResourceCollection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final String resourceCode) {
    // Get the resource definition from HAPI.
    final ResourceDefinition definition =
        FhirDefinitionContext.of(fhirContext).findResourceDefinition(resourceCode);

    // We use a literal column as the resource value - the actual value is not important.
    // But the non-null value indicates that the resource should be included in any result.
    return new ResourceCollection(
        columnRepresentation,
        Optional.empty(),
        getFhirType(resourceCode),
        Optional.of(definition),
        definition);
  }

  @Nonnull
  @Override
  protected ColumnRepresentation getFid() {
    return getColumn().traverse(ExtensionSupport.FID_FIELD_NAME());
  }

  @Nonnull
  @Override
  public Collection copyWith(@Nonnull final ColumnRepresentation newValue) {
    return new ResourceCollection(
        newValue, getType(), getFhirType(), getDefinition(), resourceDefinition);
  }

  /**
   * Returns a column that can be used as a key for joining to this resource type. The key is
   * constructed as "ResourceType/id" (e.g., "Patient/123") to match the format used by FHIR
   * references. This ensures that {@code getResourceKey()} returns a value compatible with {@code
   * getReferenceKey()} for joining resources to their references.
   *
   * @return A collection containing the resource key
   */
  @Nonnull
  public Collection getKeyCollection() {
    final String prefix = resourceDefinition.getResourceCode() + "/";
    final ColumnRepresentation idColumn =
        getColumn().traverse("id", Optional.of(FHIRDefinedType.STRING));
    return StringCollection.build(idColumn.transform(id -> concat(lit(prefix), id)));
  }

  /**
   * Returns a new collection representing just the elements of this collection with the specified
   * type.
   *
   * @param type The type of element to return
   * @return A new collection representing just the elements of this collection with the specified
   *     type
   */
  @Nonnull
  @Override
  public Collection filterByType(@Nonnull final TypeSpecifier type) {
    return type.asResourceType()
        .map(ResourceType::toCode)
        .filter(getResourceDefinition().getResourceCode()::equals)
        .map(s -> (Collection) this)
        .orElse(EmptyCollection.getInstance());
  }
}
