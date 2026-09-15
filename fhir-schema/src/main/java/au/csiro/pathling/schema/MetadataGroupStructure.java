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

package au.csiro.pathling.schema;

import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The canonical structure of the metadata group that accompanies a primitive, carrying the id and
 * the extensions the primitive itself has nowhere to put.
 *
 * <p>The group has no definition element of its own, so its order is declared rather than derived.
 * The extensions it carries take their shape from the extension element of the node the primitive
 * belongs to, which is the same definition its complex siblings use.
 */
public final class MetadataGroupStructure implements CanonicalStructure {

  @Nonnull private final Optional<ElementDefinition> extensionElement;

  @Nonnull private final Map<Object, DefinitionCanonicalStructure> memo;

  MetadataGroupStructure(
      @Nonnull final Optional<ElementDefinition> extensionElement,
      @Nonnull final Map<Object, DefinitionCanonicalStructure> memo) {
    this.extensionElement = extensionElement;
    this.memo = memo;
  }

  @Override
  @Nonnull
  public List<String> fieldOrder() {
    return LayoutFields.METADATA_GROUP_FIELDS;
  }

  @Override
  @Nonnull
  public Optional<CanonicalStructure> field(@Nonnull final String name) {
    return extensionStructure(name).map(CanonicalStructure.class::cast);
  }

  /**
   * Returns the definition of the element the extensions of this group take their shape from.
   *
   * @return the extension element, or empty where the enclosing node carries no extensions
   */
  @Nonnull
  public Optional<ElementDefinition> getExtensionElement() {
    return extensionElement;
  }

  /**
   * Returns the structure of the extensions of this group.
   *
   * @param name the name of the field to descend into
   * @return the structure beneath it, or empty where it is not the extension field
   */
  @Nonnull
  public Optional<DefinitionCanonicalStructure> extensionStructure(@Nonnull final String name) {
    if (!LayoutFields.METADATA_GROUP_EXTENSION.equals(name)) {
      return Optional.empty();
    }
    return extensionElement.map(element -> DefinitionCanonicalStructure.of(element, false, memo));
  }
}
