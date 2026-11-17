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
import au.csiro.pathling.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * An element definition under which a primitive's id and extensions are read (see {@link
 * PrimitiveMetadata}). The children it gives are wrapped in turn. Where it is a primitive's, it
 * also gives the primitive's {@code id} and {@code extension}.
 */
@ToString
@EqualsAndHashCode
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public final class MetadataElementDefinition implements ElementDefinition {

  @Nonnull private final ElementDefinition delegate;

  @Nonnull @ToString.Exclude @EqualsAndHashCode.Exclude private final PrimitiveMetadata metadata;

  /**
   * Returns whether this is the definition of a primitive, held with its id and extensions.
   *
   * @return true where this defines a primitive
   */
  boolean isPrimitive() {
    return PrimitiveMetadata.isPrimitive(getFhirType());
  }

  /**
   * Returns the definition of the extensions of this primitive, wrapped so that a primitive in an
   * extension is read with its id and extensions too.
   *
   * @return the definition of the primitive's extensions
   */
  @Nonnull
  public ElementDefinition getExtensionDefinition() {
    return metadata.wrapElement(metadata.getExtensionDefinition());
  }

  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    if (isPrimitive()) {
      // A primitive's definition declares no children, but its id and extensions are read.
      return switch (name) {
        case PrimitiveMetadata.ID -> Optional.of(metadata.getIdDefinition());
        case PrimitiveMetadata.EXTENSION -> Optional.of(getExtensionDefinition());
        default -> Optional.empty();
      };
    }
    return delegate.getChildElement(name).map(metadata::wrapChild);
  }

  @Override
  @Nonnull
  public List<ChildDefinition> getChildren() {
    // The children of a primitive are not listed, as they are not of the FHIR definition: a
    // traversal of every child, such as descendants(), does not reach them.
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

  @Override
  @Nonnull
  public String getElementName() {
    return delegate.getElementName();
  }

  @Override
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return delegate.getFhirType();
  }

  @Override
  public int getMaxCardinality() {
    return delegate.getMaxCardinality();
  }

  @Override
  public boolean isRepeating() {
    return delegate.isRepeating();
  }

  @Override
  public boolean isChoiceElement() {
    return delegate.isChoiceElement();
  }
}
