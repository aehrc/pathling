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
import au.csiro.pathling.definition.FhirType;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Finds the content the bounds configured for the dense mode would drop (FR-017).
 *
 * <p>It is the answer to the question {@link StrictnessCheck} deliberately does not ask. That check
 * judges a source against the canonical structure, which is the definitions and nothing else, so it
 * says nothing about an element the definitions describe and the nesting depth, the extension
 * switch or the enabled open types leave out of the schema. Conflating the two would report a bound
 * as undescribed content, which is untrue and points the caller at the wrong remedy: the bound is
 * raised, the data is not corrected (decision 59).
 *
 * <p>It therefore compares the source against the derived schema, and only where the definitions
 * have already accounted for the content. A name the definitions do not describe is another check's
 * finding and is passed over here, so that one piece of content never yields two findings.
 *
 * <p>The pruned mode has no use for this. A schema fitted to the data carries what the data
 * carried, and the bounds do not apply to it at all (FR-044).
 */
public final class BoundsCheck {

  /** Reported by {@link StrictnessCheck}, so it is not reported again as a bound. */
  @Nonnull private static final String CONTAINED = "contained";

  @Nonnull private final DefinitionCanonicalStructure canonical;

  @Nonnull private final StructType derived;

  @Nonnull private final String resourceType;

  private BoundsCheck(
      @Nonnull final DefinitionCanonicalStructure canonical,
      @Nonnull final StructType derived,
      @Nonnull final String resourceType) {
    this.canonical = canonical;
    this.derived = derived;
    this.resourceType = resourceType;
  }

  /**
   * Returns a check comparing what the definitions describe against what the dense schema carries.
   *
   * @param canonical the canonical structure of the resource type
   * @param derived the dense schema derived for it
   * @param resourceType the type of the resource, which roots the reported paths
   * @return the check
   */
  @Nonnull
  public static BoundsCheck of(
      @Nonnull final DefinitionCanonicalStructure canonical,
      @Nonnull final StructType derived,
      @Nonnull final String resourceType) {
    return new BoundsCheck(canonical, derived, resourceType);
  }

  /**
   * Returns the content of a source that the configured bounds would drop.
   *
   * @param observed the schema the source was read with
   * @return the findings, in the order they were reached
   */
  @Nonnull
  public List<NonConformantContent> check(@Nonnull final StructType observed) {
    final List<NonConformantContent> findings = new ArrayList<>();
    check(canonical, derived, observed, resourceType, true, findings);
    return List.copyOf(findings);
  }

  /** Walks the keys observed at one node, against the fields the derived schema has there. */
  private void check(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType carried,
      @Nonnull final StructType observed,
      @Nonnull final String path,
      final boolean resource,
      @Nonnull final List<NonConformantContent> findings) {
    for (final StructField field : observed.fields()) {
      if (resource && CONTAINED.equals(field.name())) {
        continue;
      }
      node.entry(field.name())
          .filter(LayoutEntry::isElement)
          .ifPresent(
              entry -> element(node, entry, carried, field, path + "." + field.name(), findings));
    }
  }

  /** Reports one observed element, or descends into it where the schema carries it. */
  private void element(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final StructType carried,
      @Nonnull final StructField field,
      @Nonnull final String path,
      @Nonnull final List<NonConformantContent> findings) {
    final Optional<StructField> derivedField = field(carried, field.name());
    if (derivedField.isEmpty()) {
      findings.add(NonConformantContent.outsideDenseBounds(path, boundThatDropsIt(entry)));
      return;
    }
    final DataType derivedValue = elementTypeOf(derivedField.orElseThrow().dataType());
    final DataType observedValue = elementTypeOf(field.dataType());
    if (derivedValue instanceof final StructType child
        && observedValue instanceof final StructType beneath) {
      node.elementStructure(entry)
          .ifPresent(structure -> check(structure, child, beneath, path, false, findings));
    }
  }

  /**
   * Names the bound that leaves an element out. The three are told apart the way the derivation
   * tells them apart: an extension by its type, an open type by the choice it came from, and the
   * nesting depth by being what remains.
   */
  @Nonnull
  private static String boundThatDropsIt(@Nonnull final LayoutEntry entry) {
    final Optional<FhirType> type = entry.getElement().flatMap(ElementDefinition::getFhirType);
    if (type.filter(FhirType.EXTENSION::equals).isPresent()) {
      return "extensions are not carried by this schema";
    }
    if (entry.isFromOpenChoice()) {
      return "this open type is not among the ones the schema expands a choice to";
    }
    return "the nesting bound stops the schema short of this element";
  }

  @Nonnull
  private static Optional<StructField> field(
      @Nonnull final StructType schema, @Nonnull final String name) {
    return Stream.of(schema.fields()).filter(field -> field.name().equals(name)).findFirst();
  }

  /** Returns the type of the values a column carries, unwrapping the array where it repeats. */
  @Nonnull
  private static DataType elementTypeOf(@Nonnull final DataType type) {
    return type instanceof final ArrayType array ? elementTypeOf(array.elementType()) : type;
  }
}
