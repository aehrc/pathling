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

import au.csiro.pathling.definition.ChildDefinition;
import au.csiro.pathling.definition.ChoiceDefinition;
import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.definition.NodeDefinition;
import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The canonical structure of the stored layout, taken from the FHIR definitions.
 *
 * <p>The order at a node is the order the definitions declare, with a choice expanded in place into
 * one field per type it can take, {@code contained} omitted because it is not represented, and the
 * layout's own fields at the positions {@link LayoutFields} declares.
 *
 * <p>The same enumeration serves the derivation of a schema, so that the order a schema is built in
 * and the order a merge imposes cannot drift apart. They are the same list read for different
 * purposes: derivation takes the elements and the metadata groups from it, and a merge takes the
 * names.
 *
 * <p>Expansion is lazy and memoised. Lazy because the definition graph is cyclic — extensions are
 * self-recursive, and a reference carries an identifier that carries a reference — so the expanded
 * tree is infinite and the depth needed is set by whatever asks. Memoised on the identity of the
 * definition, not on the FHIR type code, because every backbone element reports the same code while
 * describing different children.
 */
public final class DefinitionCanonicalStructure implements CanonicalStructure {

  /** The element that carries the resources a resource contains, which this layout omits. */
  @Nonnull private static final String CONTAINED = "contained";

  @Nonnull private final NodeDefinition node;

  private final boolean resource;

  @Nonnull private final Map<Object, DefinitionCanonicalStructure> memo;

  @Nonnull private Optional<List<LayoutEntry>> entries = Optional.empty();

  @Nonnull private Optional<Map<String, LayoutEntry>> index = Optional.empty();

  private DefinitionCanonicalStructure(
      @Nonnull final NodeDefinition node,
      final boolean resource,
      @Nonnull final Map<Object, DefinitionCanonicalStructure> memo) {
    this.node = node;
    this.resource = resource;
    this.memo = memo;
  }

  /**
   * Returns the canonical structure of a resource type.
   *
   * @param definitions the definitions to take the structure from
   * @param resourceType the type of the resource
   * @return the canonical structure of that resource
   */
  @Nonnull
  public static DefinitionCanonicalStructure forResource(
      @Nonnull final DefinitionContext definitions, @Nonnull final String resourceType) {
    return of(definitions.findResourceDefinition(resourceType), true);
  }

  /**
   * Returns the canonical structure of a node, at the root of a fresh memo.
   *
   * @param node the definition of the node
   * @param resource whether the node is a resource, and therefore carries the resource type field
   * @return the canonical structure of that node
   */
  @Nonnull
  public static DefinitionCanonicalStructure of(
      @Nonnull final NodeDefinition node, final boolean resource) {
    final Map<Object, DefinitionCanonicalStructure> memo = new ConcurrentHashMap<>();
    final DefinitionCanonicalStructure structure =
        new DefinitionCanonicalStructure(node, resource, memo);
    memo.put(node.getTypeIdentity(), structure);
    return structure;
  }

  /**
   * Returns the canonical structure of a node within an existing memo, so that a type reached again
   * by another path is the structure already built for it.
   *
   * @param node the definition of the node
   * @param resource whether the node is a resource
   * @param memo the memo shared by the structure this node belongs to
   * @return the canonical structure of that node
   */
  @Nonnull
  static DefinitionCanonicalStructure of(
      @Nonnull final NodeDefinition node,
      final boolean resource,
      @Nonnull final Map<Object, DefinitionCanonicalStructure> memo) {
    return memo.computeIfAbsent(
        node.getTypeIdentity(), unused -> new DefinitionCanonicalStructure(node, resource, memo));
  }

  /**
   * Returns the fields at this node, in canonical order, as the entries a schema derivation needs
   * rather than as names alone.
   *
   * @return the fields at this node
   */
  @Nonnull
  public List<LayoutEntry> entries() {
    if (entries.isEmpty()) {
      entries = Optional.of(buildEntries());
    }
    return entries.orElseThrow();
  }

  @Override
  @Nonnull
  public List<String> fieldOrder() {
    return entries().stream().map(LayoutEntry::getName).toList();
  }

  @Override
  @Nonnull
  public Optional<CanonicalStructure> field(@Nonnull final String name) {
    final Optional<LayoutEntry> entry = entry(name);
    return entry
        .filter(LayoutEntry::isElement)
        .flatMap(this::elementStructure)
        .map(CanonicalStructure.class::cast)
        .or(
            () ->
                entry
                    .filter(LayoutEntry::isMetadataGroup)
                    .map(unused -> new MetadataGroupStructure(extensionElement(), memo)));
  }

  /**
   * Returns the entry for a named field, which is how a derivation reaches the definition behind a
   * name it has already ordered.
   *
   * @param name the name of the field
   * @return the entry, or empty where this node has no such field
   */
  @Nonnull
  public Optional<LayoutEntry> entry(@Nonnull final String name) {
    if (index.isEmpty()) {
      final Map<String, LayoutEntry> byName = new LinkedHashMap<>();
      entries().forEach(entry -> byName.put(entry.getName(), entry));
      index = Optional.of(byName);
    }
    return Optional.ofNullable(index.orElseThrow().get(name));
  }

  /**
   * Returns the structure beneath an element entry, which is empty where the element is a primitive
   * and therefore has no structure.
   *
   * @param entry the entry to descend into
   * @return the structure beneath it
   */
  @Nonnull
  public Optional<DefinitionCanonicalStructure> elementStructure(@Nonnull final LayoutEntry entry) {
    return entry
        .getElement()
        .filter(element -> element.getFhirType().filter(PrimitiveTypes::isPrimitive).isEmpty())
        .map(element -> of(element, false, memo));
  }

  /**
   * Returns the structure of the metadata group beside a primitive, which needs the extension
   * element of this node in order to describe the extensions it carries.
   *
   * @param name the name of the metadata group
   * @return the structure of the group, or empty where this node has no such group
   */
  @Nonnull
  public Optional<MetadataGroupStructure> metadataGroup(@Nonnull final String name) {
    return entry(name)
        .filter(LayoutEntry::isMetadataGroup)
        .map(unused -> new MetadataGroupStructure(extensionElement(), memo));
  }

  /**
   * Returns the extension element of this node, which is the definition every extension carried
   * here takes its shape from, including the extensions within a metadata group.
   *
   * @return the extension element, or empty where this node carries no extensions
   */
  @Nonnull
  public Optional<ElementDefinition> extensionElement() {
    return entries().stream()
        .filter(entry -> LayoutFields.METADATA_GROUP_EXTENSION.equals(entry.getName()))
        .findFirst()
        .flatMap(LayoutEntry::getElement);
  }

  @Nonnull
  private List<LayoutEntry> buildEntries() {
    final List<LayoutEntry> result = new ArrayList<>();
    if (resource) {
      result.add(LayoutEntry.resourceType());
    }
    for (final ChildDefinition child : node.getChildren()) {
      if (child instanceof final ChoiceDefinition choice) {
        // A choice is reported once under its unqualified name, while the layout carries one field
        // per type, in the order the choice reports its types.
        choice
            .getAllChildTypes()
            .forEach(variant -> addElement(result, variant, choice.isOpenType()));
      } else if (child instanceof final ElementDefinition element) {
        addElement(result, element, false);
      }
    }
    return List.copyOf(result);
  }

  private void addElement(
      @Nonnull final List<LayoutEntry> result,
      @Nonnull final ElementDefinition element,
      final boolean fromOpenChoice) {
    final String name = element.getElementName();
    if (resource && CONTAINED.equals(name)) {
      // Contained resources are not represented in this layout.
      return;
    }
    final Optional<FHIRDefinedType> type = element.getFhirType();
    if (type.isEmpty()) {
      // An element the definitions do not give a type cannot be represented.
      return;
    }
    result.add(LayoutEntry.element(name, element, fromOpenChoice));
    if (PrimitiveTypes.isPrimitive(type.orElseThrow())) {
      result.add(
          LayoutEntry.metadataGroup(LayoutFields.metadataGroupName(name), element, fromOpenChoice));
    }
    LayoutFields.annotationNames(name, type.orElseThrow())
        .forEach(
            annotation -> result.add(LayoutEntry.annotation(annotation, element, fromOpenChoice)));
  }
}
