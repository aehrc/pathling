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
 * A FHIRPath ResourceResolver that can handle joins.
 */
@EqualsAndHashCode(callSuper = true)
@Value
public class ManyResourceResolver extends BaseResourceResolver {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  FhirContext fhirContext;

  @Nonnull
  DataSource dataSource;

  @Nonnull
  List<JoinSet> joinSets;

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


  @Override
  public @Nonnull Collection resolveJoin(@Nonnull final ReferenceCollection referenceCollection) {
    final ResourceTypeSet referenceTypes = referenceCollection.getReferenceTypes();
    return referenceTypes.asSingleResourceType()
        .map(referenceType -> (Collection) resolveTypedJoin(referenceCollection, referenceType))
        .orElseGet(() -> new MixedResourceCollection(referenceCollection, this::resolveTypedJoin));
  }

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

  @Nonnull
  @Override
  public Dataset<Row> createView() {
    return JoinResolver.of(subjectResource, fhirContext, dataSource)
        .resolveJoins(joinSets);
  }
}
