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

package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.fhir.FhirUtils.isKnownResource;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.collection.mixed.MixedResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.ResourceTypeSet;
import au.csiro.pathling.fhirpath.execution.JoinTag.ResolveTag;
import au.csiro.pathling.fhirpath.execution.JoinTag.ResourceTag;
import au.csiro.pathling.fhirpath.execution.JoinTag.ReverseResolveTag;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A sophisticated implementation of {@link BaseResourceResolver} that supports complex joins
 * between FHIR resources.
 * <p>
 * This resolver is designed for advanced FHIRPath evaluation scenarios where expressions traverse
 * resource boundaries through references. It supports:
 * <ul>
 *   <li>Forward resolves (following references from one resource to another)</li>
 *   <li>Reverse resolves (finding resources that reference a particular resource)</li>
 *   <li>Access to foreign resources (resources other than the subject resource)</li>
 *   <li>Mixed resource collections (collections containing multiple resource types)</li>
 * </ul>
 * <p>
 * The resolver uses {@link JoinSet}s to define the relationships between resources and
 * {@link JoinResolver} to perform the actual joins between datasets.
 */
@EqualsAndHashCode(callSuper = true)
@Value
public class ManyResourceResolver extends BaseResourceResolver {

  /**
   * The primary resource type being queried.
   */
  @Nonnull
  ResourceType subjectResource;

  /**
   * The FHIR context used for resource definitions.
   */
  @Nonnull
  FhirContext fhirContext;

  /**
   * The data source from which to read resource data.
   */
  @Nonnull
  DataSource dataSource;

  /**
   * The set of join definitions that describe the relationships between resources.
   */
  @Nonnull
  List<JoinSet> joinSets;

  /**
   * Resolves a reference to a specific resource type.
   * <p>
   * This method is used to implement the FHIRPath resolve() function for a specific resource type.
   * It:
   * <ol>
   *   <li>Validates that the reference can point to the specified resource type</li>
   *   <li>Creates a join tag for the reference</li>
   *   <li>Builds a ResourceCollection that uses the join tag to access the referenced resources</li>
   * </ol>
   *
   * @param referenceCollection The collection of references to resolve
   * @param referenceType The specific resource type to resolve to
   * @return A ResourceCollection containing the resolved resources
   * @throws IllegalArgumentException if the reference cannot point to the specified resource type
   */
  @Nonnull
  ResourceCollection resolveTypedJoin(
      @Nonnull final ReferenceCollection referenceCollection,
      @Nonnull final ResourceType referenceType) {

    if (!referenceCollection.getReferenceTypes().contains(referenceType)) {
      throw new IllegalArgumentException(
          "Reference type does not match. Expected: " + referenceType + " but got: "
              + referenceCollection.getReferenceTypes());
    }
    final JoinTag resolveTag = ResolveTag.of(referenceType);
    return ResourceCollection.build(
        referenceCollection
            .getKeyCollection(Optional.empty()).getColumn()
            .applyTo(resolveTag.getTagColumn()),
        fhirContext, referenceType);
  }


  /**
   * {@inheritDoc}
   * <p>
   * This implementation handles both single-type and mixed-type references:
   * <ul>
   *   <li>For references to a single resource type, it delegates to {@link #resolveTypedJoin}</li>
   *   <li>For references that could point to multiple resource types, it creates a
   *       {@link MixedResourceCollection} that can handle any of the possible types</li>
   * </ul>
   */
  @Override
  public @Nonnull Collection resolveJoin(@Nonnull final ReferenceCollection referenceCollection) {
    final ResourceTypeSet referenceTypes = referenceCollection.getReferenceTypes();
    return referenceTypes.asSingleResourceType()
        .map(referenceType -> (Collection) resolveTypedJoin(referenceCollection, referenceType))
        .orElseGet(() -> new MixedResourceCollection(referenceCollection, this::resolveTypedJoin));
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation:
   * <ol>
   *   <li>Validates that the child resource type is known</li>
   *   <li>Creates a reverse resolve join tag for the relationship</li>
   *   <li>Builds a ResourceCollection that uses the join tag to access the child resources</li>
   * </ol>
   * <p>
   * For example, in Patient.reverseResolve(Condition.subject), this method creates a collection
   * of Condition resources that reference each Patient through their subject field.
   */
  @Nonnull
  @Override
  public ResourceCollection resolveReverseJoin(@Nonnull final ResourceCollection parentResource,
      @Nonnull final String childResourceCode,
      @Nonnull final String childReferenceToParentFhirpath) {

    if (!isKnownResource(childResourceCode, fhirContext)) {
      throw new IllegalArgumentException("Unknown child resource type: " + childResourceCode);
    }
    final ResourceType childResourceType = ResourceType.fromCode(childResourceCode);
    final JoinTag reverseResolveTag = ReverseResolveTag.of(childResourceType,
        childReferenceToParentFhirpath);
    return ResourceCollection.build(
        parentResource.getKeyCollection()
            .getColumn().applyTo(reverseResolveTag.getTagColumn()),
        fhirContext, childResourceType);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation provides access to foreign resources (resources other than the subject
   * resource) by:
   * <ol>
   *   <li>Validating that the resource type is known</li>
   *   <li>Creating a resource tag for the resource type</li>
   *   <li>Building a ResourceCollection that uses the resource tag to access the resources</li>
   * </ol>
   * <p>
   * Foreign resources are represented as arrays of structs in the dataset, with one array
   * containing all instances of the resource type.
   */
  @Override
  @Nonnull
  Optional<ResourceCollection> resolveForeignResource(@Nonnull final String resourceCode) {
    if (isKnownResource(resourceCode, fhirContext)) {
      final ResourceType resourceType = ResourceType.fromCode(resourceCode);
      final ResourceTag resourceTag = ResourceTag.of(resourceType);
      return Optional.of(ResourceCollection.build(
          new DefaultRepresentation(resourceTag.getTagColumn()),
          getFhirContext(), resourceType));
    } else {
      return Optional.empty();
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation uses {@link JoinResolver} to create a dataset that includes all resources
   * and joins defined in the {@link #joinSets}. The resulting dataset contains:
   * <ul>
   *   <li>The subject resource as a struct column</li>
   *   <li>Foreign resources as array columns</li>
   *   <li>Forward resolve joins as map columns with resource IDs as keys</li>
   *   <li>Reverse resolve joins as map columns with arrays of child resources as values</li>
   * </ul>
   */
  @Nonnull
  @Override
  public Dataset<Row> createView() {
    return JoinResolver.of(subjectResource, fhirContext, dataSource)
        .resolveJoins(joinSets);
  }
}
