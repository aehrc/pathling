/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.definition.PrimitiveMetadata;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * A resource resolver whose resources are read with their primitives' ids and extensions (decision
 * 141, D14): it resolves what another resolver does, under definitions that read them (see {@link
 * PrimitiveMetadata}).
 *
 * @param delegate the resolver that resolves the resources
 * @param metadata the definitions of a primitive's id and extensions
 */
record PrimitiveMetadataResolver(
    @Nonnull ResourceResolver delegate, @Nonnull PrimitiveMetadata metadata)
    implements ResourceResolver {

  @Override
  @Nonnull
  public String getSubjectResourceCode() {
    return delegate.getSubjectResourceCode();
  }

  @Override
  @Nonnull
  public ResourceCollection resolveSubjectResource() {
    return metadata.apply(delegate.resolveSubjectResource());
  }

  @Override
  @Nonnull
  public Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode) {
    return delegate.resolveResource(resourceCode).map(metadata::apply);
  }
}
