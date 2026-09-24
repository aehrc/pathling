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

import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Finds the content a source carries that this layout does not store (FR-006).
 *
 * <p>It compares the keys the source carried against the fields the layout has for them, taken from
 * the canonical structure. It cannot be delegated to the reader, which skips a key it was not asked
 * for in every mode, {@code FAILFAST} included. Nor can it be built on whether the definition
 * library resolves a name: that library is permissive about aliases, and resolves names for which
 * this layout has no column — a choice admitting a reference resolves a name per target resource
 * type, while the layout carries the one reference field FHIR declares — so a check built on
 * resolution would accept content the layout then drops.
 *
 * <p>Nothing here decides what happens to a finding. The content is ignored whatever its kind, and
 * the findings are what the warning naming it is made from (decision 68).
 */
public final class StrictnessCheck {

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
      if (resource && LayoutFields.CONTAINED.equals(field.name())) {
        findings.add(NonConformantContent.containedResource(here));
      } else if (entry.filter(e -> e.isElement() || e.isResourceType()).isPresent()) {
        checkElement(node, entry.orElseThrow(), field.dataType(), here, findings);
      } else if (entry.filter(LayoutEntry::isMetadataGroup).isPresent()) {
        checkMetadataGroup(entry.orElseThrow(), field.dataType(), here, findings);
      } else {
        // Only the fields a source can carry are matched. Any other, an annotation included, is a
        // field the layout derives rather than one the definitions describe, so a source key of
        // that name is undescribed content and never read as the element it accompanies.
        findings.add(NonConformantContent.undescribedContent(here));
      }
    }
  }

  /**
   * Checks the metadata group beside a primitive. A group in the shape FHIR gives it is reported as
   * primitive metadata: it is not written until M5, so its content is lost, and FR-017's carve-out
   * requires that loss to be detectable. It is recognised from the canonical structure rather than
   * from the element beside it, because a data absent reason carries the group with no value of its
   * own. A group in any other shape contradicts the definitions, and keeps nothing of the element
   * it accompanies (decision 72).
   */
  private static void checkMetadataGroup(
      @Nonnull final LayoutEntry group,
      @Nonnull final DataType observed,
      @Nonnull final String path,
      @Nonnull final List<NonConformantContent> findings) {
    final boolean repeating = group.isRepeating();
    if (isConformingMetadataGroup(repeating, observed)) {
      findings.add(NonConformantContent.primitiveMetadata(path));
    } else {
      findings.add(NonConformantContent.shapeMismatch(path, metadataGroupShape(repeating)));
    }
  }

  /** Returns the shape FHIR gives the metadata group of a primitive, as a finding states it. */
  @Nonnull
  private static String metadataGroupShape(final boolean repeating) {
    return repeating
        ? "the metadata group of a repeating primitive must be an array of objects"
        : "the metadata group of a singular primitive must be an object";
  }

  /**
   * Returns whether the column a metadata group was read into has the outer shape FHIR gives the
   * group: an object beside a singular primitive, and an array of objects beside a repeating one.
   * What the objects hold is not examined; that is checked where the group is written (M5).
   *
   * @param repeating whether the primitive the group accompanies repeats
   * @param observed the type the group was read with
   * @return whether the group has that shape
   */
  static boolean isConformingMetadataGroup(
      final boolean repeating, @Nonnull final DataType observed) {
    return repeating
        ? observed instanceof final ArrayType array && array.elementType() instanceof StructType
        : observed instanceof StructType;
  }

  /**
   * Returns whether the column an element was read into has the cardinality the definitions give
   * it: one array level where the element repeats, and none where it is singular. FHIR JSON has no
   * arrays of arrays, so an array whose items are arrays contradicts a repeating element as much as
   * a single value does.
   *
   * @param repeating whether the definitions declare the element repeating
   * @param observed the type the element was read with
   * @return whether the element has that cardinality
   */
  static boolean isConformingCardinality(
      final boolean repeating, @Nonnull final DataType observed) {
    return repeating
        ? observed instanceof final ArrayType array && !(array.elementType() instanceof ArrayType)
        : !(observed instanceof ArrayType);
  }

  /** Returns how an element's cardinality contradicts the definitions, as a finding states it. */
  @Nonnull
  private static String cardinalityMismatch(
      final boolean repeating, @Nonnull final DataType observed) {
    if (!repeating) {
      return "the definitions declare a singular element and the source carries an array";
    }
    return observed instanceof ArrayType
        ? "the definitions declare a repeating element and the source carries an array of arrays"
        : "the definitions declare a repeating element and the source carries a single value";
  }

  /**
   * Returns how the shape of an element contradicts the definitions, as a finding states it, or
   * empty where it has the shape they declare. The source contradicts them where it repeated what
   * they declare singular, gave a single value or an array of arrays where they declare a repeat,
   * or gave a structure where they declare a leaf and the other way about. The transform stores an
   * element only where this is empty, so what it drops and what is reported cannot disagree.
   *
   * @param node the canonical structure of the node carrying the element
   * @param entry the element
   * @param observed the type the element was read with
   * @return the contradiction, or empty where there is none
   */
  @Nonnull
  static Optional<String> shapeMismatch(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType observed) {
    final boolean repeating = entry.isRepeating();
    if (!isConformingCardinality(repeating, observed)) {
      return Optional.of(cardinalityMismatch(repeating, observed));
    }
    final boolean structure = node.elementStructure(entry).isPresent();
    if (structure == elementTypeOf(observed) instanceof StructType) {
      return Optional.empty();
    }
    return Optional.of(
        structure
            ? "the definitions declare a structure and the source carries a value"
            : "the definitions declare a primitive and the source carries a structure");
  }

  /** Checks one element the definitions describe, descending into it where it is a structure. */
  private void checkElement(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType observed,
      @Nonnull final String path,
      @Nonnull final List<NonConformantContent> findings) {
    final Optional<String> mismatch = shapeMismatch(node, entry, observed);
    if (mismatch.isPresent()) {
      findings.add(NonConformantContent.shapeMismatch(path, mismatch.orElseThrow()));
      return;
    }
    final DataType value = elementTypeOf(observed);
    node.elementStructure(entry)
        .ifPresentOrElse(
            child -> check(child, (StructType) value, path, false, findings),
            () -> checkEncoding(entry, value, path, findings));
  }

  /**
   * Checks the type inference gave a primitive element against what its converter accepts, which is
   * how a JSON encoding that contradicts the definitions shows.
   */
  private static void checkEncoding(
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType observed,
      @Nonnull final String path,
      @Nonnull final List<NonConformantContent> findings) {
    entry
        .primitiveType()
        .filter(
            type -> PrimitiveConverters.forType(type).filter(c -> c.accepts(observed)).isEmpty())
        .ifPresent(
            type ->
                findings.add(
                    NonConformantContent.encodingMismatch(
                        path,
                        "the definitions declare a "
                            + type.toCode()
                            + " and the source carries a "
                            + observed.simpleString()
                            + " column")));
  }

  /**
   * Returns the type of the values a column carries, unwrapping the array where it repeats. Every
   * level is unwrapped, so that a column in a shape the definitions contradict still yields the
   * structure beneath it.
   *
   * @param type the type of the column
   * @return the type of its values
   */
  @Nonnull
  static DataType elementTypeOf(@Nonnull final DataType type) {
    return type instanceof final ArrayType array ? elementTypeOf(array.elementType()) : type;
  }
}
