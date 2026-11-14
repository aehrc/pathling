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

package au.csiro.pathling.fhirpath.definition;

import au.csiro.pathling.definition.ChildDefinition;
import au.csiro.pathling.definition.ChoiceDefinition;
import au.csiro.pathling.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A choice definition under which a primitive's id and extensions are read (see {@link
 * PrimitiveMetadata}). The types it gives are wrapped in turn.
 */
@ToString
@EqualsAndHashCode
@AllArgsConstructor(access = AccessLevel.PACKAGE)
final class MetadataChoiceDefinition implements ChoiceDefinition {

  @Nonnull private final ChoiceDefinition delegate;

  @Nonnull @ToString.Exclude @EqualsAndHashCode.Exclude private final PrimitiveMetadata metadata;

  @Override
  @Nonnull
  public Optional<ElementDefinition> getChildByType(@Nonnull final String type) {
    return delegate.getChildByType(type).map(metadata::wrapElement);
  }

  @Override
  @Nonnull
  public List<ElementDefinition> getAllChildTypes() {
    return delegate.getAllChildTypes().stream().map(metadata::wrapElement).toList();
  }

  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return delegate.getChildElement(name).map(metadata::wrapChild);
  }

  @Override
  @Nonnull
  public List<ChildDefinition> getChildren() {
    return delegate.getChildren().stream().map(metadata::wrapChild).toList();
  }

  @Override
  @Nonnull
  public Object getTypeIdentity() {
    return delegate.getTypeIdentity();
  }

  @Override
  public boolean isFhirDefinition() {
    return delegate.isFhirDefinition();
  }

  @Override
  @Nonnull
  public String getName() {
    return delegate.getName();
  }
}
