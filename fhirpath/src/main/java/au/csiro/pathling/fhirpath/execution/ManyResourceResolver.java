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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.collection.mixed.MixedResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.ResourceTypeSet;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.DataFormatException;
import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.util.List;
import java.util.Optional;

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
    // TODO: get from the reference collection
    final JoinTag valueTag = JoinTag.ResolveTag.of(referenceType);

    return ResourceCollection.build(
        referenceCollection.getColumn().traverse("reference")
            .applyTo(functions.col(valueTag.getTag())),
        fhirContext, referenceType);
  }


  @Override
  public @Nonnull Collection resolveJoin(
      @Nonnull final ReferenceCollection referenceCollection) {
    final ResourceTypeSet referenceTypes = referenceCollection.getReferenceTypes();
    return referenceTypes.asSingleResourceType()
        .map(referenceType -> (Collection) resolveTypedJoin(referenceCollection, referenceType))
        .orElseGet(() -> new MixedResourceCollection(referenceCollection,
            this::resolveTypedJoin));
  }

  @Nonnull
  @Override
  public ResourceCollection resolveReverseJoin(@Nonnull final ResourceCollection parentResource,
      @Nonnull final String expression) {

    // TODO: implement this
    final String resourceName = expression.split("\\.")[0];
    final String masterKeyPath = expression.split("\\.")[1];
    final ResourceType childResourceType = ResourceType.fromCode(resourceName);

    final JoinTag valueTag = JoinTag.ReverseResolveTag.of(childResourceType, masterKeyPath);

    return ResourceCollection.build(
        parentResource.getColumn().traverse("id_versioned")
            .applyTo(functions.col(valueTag.getTag())),
        fhirContext, childResourceType);
  }


  @Override
  @Nonnull
  Optional<ResourceCollection> resolveForeignResource(@Nonnull final String resourceCode) {
    try {
      final RuntimeResourceDefinition definition = fhirContext.getResourceDefinition(
          resourceCode);
      final ResourceType resourceType = ResourceType.fromCode(resourceCode);
      final ResourceCollection resourcCollecttion = ResourceCollection.build(
          new DefaultRepresentation(functions.col("@" + resourceType.toCode())),
          getFhirContext(), resourceType);
      return Optional.of(resourcCollecttion);
    } catch (DataFormatException e) {
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
