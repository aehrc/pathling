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

package au.csiro.pathling.io.transform;

import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Finds the content a source carries that this layout does not store (FR-006, FR-018).
 *
 * <p>It compares the keys the source carried against the fields the layout has for them, taken from
 * the canonical structure. It cannot be delegated to the reader, which skips a key it was not asked
 * for in every mode, {@code FAILFAST} included. Nor can it be built on whether the definition
 * library resolves a name: that library is permissive about aliases, and resolves names for which
 * this layout has no column — a choice admitting a reference resolves a name per target resource
 * type, while the layout carries the one reference field FHIR declares — so a check built on
 * resolution would accept content the layout then drops.
 *
 * <p>The canonical structure is the right authority rather than a derived schema, because a derived
 * schema in the dense mode omits fields for reasons of the configured bounds rather than because
 * the definitions are silent about them, and those two losses are different things.
 *
 * <p>Nothing here decides what happens to a finding. Every kind is detected in both settings of the
 * strictness switch, which governs only whether the findings raise.
 */
public final class StrictnessCheck {

  /** The element carrying the resources a resource contains, which this layout does not store. */
  @Nonnull private static final String CONTAINED = "contained";

  @Nonnull private final DefinitionCanonicalStructure canonical;

  @Nonnull private final String resourceType;

  private StrictnessCheck(
      @Nonnull final DefinitionCanonicalStructure canonical, @Nonnull final String resourceType) {
    this.canonical = canonical;
    this.resourceType = resourceType;
  }

  /**
   * Returns a check over the canonical structure of a resource type.
   *
   * @param canonical the canonical structure of the resource type
   * @param resourceType the type of the resource, which roots the reported paths
   * @return the check
   */
  @Nonnull
  public static StrictnessCheck of(
      @Nonnull final DefinitionCanonicalStructure canonical, @Nonnull final String resourceType) {
    return new StrictnessCheck(canonical, resourceType);
  }

  /**
   * Returns the content of a source that this layout does not store.
   *
   * @param observed the schema the source was read with
   * @return the findings, in the order they were reached
   */
  @Nonnull
  public List<NonConformantContent> check(@Nonnull final StructType observed) {
    final List<NonConformantContent> findings = new ArrayList<>();
    check(canonical, observed, resourceType, true, findings);
    return List.copyOf(findings);
  }

  /** Walks the keys observed at one node, against the fields the layout has there. */
  private void check(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType observed,
      @Nonnull final String path,
      final boolean resource,
      @Nonnull final List<NonConformantContent> findings) {
    for (final StructField field : observed.fields()) {
      final String here = path + "." + field.name();
      final Optional<LayoutEntry> entry = node.entry(field.name());
      if (resource && CONTAINED.equals(field.name())) {
        findings.add(NonConformantContent.containedResource(here));
      } else if (entry.isEmpty()) {
        findings.add(NonConformantContent.undescribedContent(here));
      } else if (entry.orElseThrow().isMetadataGroup()) {
        // The group is derived but not yet populated, so its content is lost; FR-017's carve-out
        // requires that loss to be detectable. It is recognised from the canonical structure rather
        // than from the element beside it, because a data absent reason carries the group with no
        // value of its own.
        findings.add(NonConformantContent.primitiveMetadata(here));
      } else {
        checkElement(node, entry.orElseThrow(), field.dataType(), here, findings);
      }
    }
  }

  /**
   * Checks one element the definitions describe, which contradicts them where the source repeated
   * what they declare singular, gave a single value where they declare a repeat, or gave a
   * structure where they declare a leaf and the other way about.
   */
  private void checkElement(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType observed,
      @Nonnull final String path,
      @Nonnull final List<NonConformantContent> findings) {
    final boolean repeating =
        entry.getElement().map(ElementDefinition::isRepeating).orElse(Boolean.FALSE);
    if (repeating != observed instanceof ArrayType) {
      findings.add(
          NonConformantContent.shapeMismatch(
              path,
              repeating
                  ? "the definitions declare a repeating element and the source carries a single"
                      + " value"
                  : "the definitions declare a singular element and the source carries an array"));
      return;
    }
    final DataType value = elementTypeOf(observed);
    final Optional<DefinitionCanonicalStructure> child = node.elementStructure(entry);
    if (child.isEmpty() && value instanceof StructType) {
      findings.add(
          NonConformantContent.shapeMismatch(
              path, "the definitions declare a primitive and the source carries a structure"));
    } else if (child.isPresent() && !(value instanceof StructType)) {
      findings.add(
          NonConformantContent.shapeMismatch(
              path, "the definitions declare a structure and the source carries a value"));
    } else if (child.isPresent()) {
      check(child.orElseThrow(), (StructType) value, path, false, findings);
    }
  }

  /** Returns the type of the values a column carries, unwrapping the array where it repeats. */
  @Nonnull
  private static DataType elementTypeOf(@Nonnull final DataType type) {
    return type instanceof final ArrayType array ? elementTypeOf(array.elementType()) : type;
  }
}
