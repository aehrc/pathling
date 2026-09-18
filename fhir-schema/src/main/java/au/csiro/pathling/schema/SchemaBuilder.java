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

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Derives the schema of a resource table from the FHIR definitions.
 *
 * <p>Types and cardinality come from the definitions and never from the data: a repeating element
 * is an array column whether the data carried one value or a thousand, and a singular element is a
 * scalar column whether or not the data repeated it (FR-012).
 *
 * <p>There is one traversal and two stopping rules. In the dense mode it stops at the configured
 * bounds: the nesting depth, whether extensions are carried, and which types an open choice expands
 * to. In the pruned mode it stops where the source has nothing, and the bounds do not apply,
 * because depth then comes from the data, extensions appear because they are present and open types
 * resolve as observed (FR-044). The dense schema is therefore the pruned derivation with pruning
 * skipped, and the two cannot diverge in type, cardinality or field order (FR-010).
 *
 * <p>The field order is the canonical one, read from {@link DefinitionCanonicalStructure} rather
 * than computed again here, so that the order a schema is built in and the order a merge imposes
 * are the same order.
 *
 * <p>Annotations are not derived. They are optional by definition, are added by the ingest
 * transform and are individually disableable; this class derives the elements the definitions
 * describe, and the metadata groups that carry the ids and extensions of primitives.
 */
public final class SchemaBuilder {

  @Nonnull private final DefinitionContext definitions;

  private final int maxNestingLevel;

  private final boolean enableExtensions;

  @Nonnull private final Set<String> enabledOpenTypes;

  private SchemaBuilder(
      @Nonnull final DefinitionContext definitions,
      final int maxNestingLevel,
      final boolean enableExtensions,
      @Nonnull final Set<String> enabledOpenTypes) {
    this.definitions = definitions;
    this.maxNestingLevel = maxNestingLevel;
    this.enableExtensions = enableExtensions;
    this.enabledOpenTypes = enabledOpenTypes;
  }

  /**
   * Returns a schema builder over a set of definitions, bounded as given.
   *
   * <p>The bounds are taken as plain values rather than as a configuration object, because they
   * belong to configuration that this module must not see.
   *
   * @param definitions the definitions to derive schemas from
   * @param maxNestingLevel how many times a type may recur within itself, in the dense mode
   * @param enableExtensions whether extensions are carried, in the dense mode
   * @param enabledOpenTypes the types an open choice expands to, in the dense mode
   * @return the schema builder
   */
  @Nonnull
  public static SchemaBuilder of(
      @Nonnull final DefinitionContext definitions,
      final int maxNestingLevel,
      final boolean enableExtensions,
      @Nonnull final Set<String> enabledOpenTypes) {
    return new SchemaBuilder(definitions, maxNestingLevel, enableExtensions, enabledOpenTypes);
  }

  /**
   * Derives the dense schema of a resource type, comprising every element the definitions describe
   * within the configured bounds.
   *
   * @param resourceType the type of the resource
   * @return the derived schema
   */
  @Nonnull
  public StructType dense(@Nonnull final String resourceType) {
    return build(
        DefinitionCanonicalStructure.forResource(definitions, resourceType),
        Optional.empty(),
        new HashMap<>());
  }

  /**
   * Derives the schema of a resource type pruned to the elements the source populates.
   *
   * @param resourceType the type of the resource
   * @param observed the schema of the source, which decides presence and nothing else
   * @return the derived schema
   */
  @Nonnull
  public StructType pruned(@Nonnull final String resourceType, @Nonnull final StructType observed) {
    return build(
        DefinitionCanonicalStructure.forResource(definitions, resourceType),
        Optional.of(SchemaPruner.of(observed)),
        new HashMap<>());
  }

  @Nonnull
  private StructType build(
      @Nonnull final DefinitionCanonicalStructure structure,
      @Nonnull final Optional<SchemaPruner> observed,
      @Nonnull final Map<Object, Integer> path) {
    final List<StructField> fields = new ArrayList<>();
    for (final LayoutEntry entry : structure.entries()) {
      if (entry.isAnnotation()) {
        // Annotations are added by the transform that writes the data, not by the derivation.
        continue;
      }
      if (entry.isResourceType()) {
        // Every conformant file carries the resource type, so it is not subject to pruning.
        fields.add(nullable(entry.getName(), DataTypes.StringType));
        continue;
      }
      if (!retained(entry, observed)) {
        continue;
      }
      final ElementDefinition element = entry.getElement().orElseThrow();
      final Optional<DataType> type =
          entry.isMetadataGroup()
              ? metadataGroupType(structure, entry, observed, path)
              : elementType(structure, entry, observed, path);
      type.ifPresent(
          dataType -> fields.add(nullable(entry.getName(), repeated(element, dataType))));
    }
    return new StructType(fields.toArray(StructField[]::new));
  }

  /** Answers whether a field survives the stopping rule that applies to the mode in force. */
  private boolean retained(
      @Nonnull final LayoutEntry entry, @Nonnull final Optional<SchemaPruner> observed) {
    if (observed.isPresent()) {
      return observed.orElseThrow().retains(entry.getName());
    }
    return withinDenseBounds(entry);
  }

  /**
   * Answers whether a field is within the bounds that apply to the dense mode. The nesting bound is
   * applied where the element is expanded rather than here, because it depends on the path taken to
   * reach it.
   */
  private boolean withinDenseBounds(@Nonnull final LayoutEntry entry) {
    final Optional<FHIRDefinedType> type =
        entry.getElement().flatMap(ElementDefinition::getFhirType);
    if (type.filter(FHIRDefinedType.EXTENSION::equals).isPresent()) {
      return enableExtensions;
    }
    if (entry.isFromOpenChoice()) {
      // An open choice may take any type the specification allows, which is a structure of
      // unusable width unless the expansion is restricted.
      return type.map(FHIRDefinedType::toCode).filter(enabledOpenTypes::contains).isPresent();
    }
    return true;
  }

  @Nonnull
  private Optional<DataType> elementType(
      @Nonnull final DefinitionCanonicalStructure structure,
      @Nonnull final LayoutEntry entry,
      @Nonnull final Optional<SchemaPruner> observed,
      @Nonnull final Map<Object, Integer> path) {
    final ElementDefinition element = entry.getElement().orElseThrow();
    final FHIRDefinedType type = element.getFhirType().orElseThrow();
    final Optional<DataType> primitive = PrimitiveTypes.storageTypeOf(type);
    if (primitive.isPresent()) {
      return primitive;
    }
    return structure
        .elementStructure(entry)
        .flatMap(child -> structureType(child, element, type, entry.getName(), observed, path))
        .map(DataType.class::cast);
  }

  /**
   * Derives the structure of a complex element, descending into whatever the source observed
   * beneath it and dropping it where nothing survives.
   */
  @Nonnull
  private Optional<StructType> structureType(
      @Nonnull final DefinitionCanonicalStructure child,
      @Nonnull final ElementDefinition element,
      @Nonnull final FHIRDefinedType type,
      @Nonnull final String name,
      @Nonnull final Optional<SchemaPruner> observed,
      @Nonnull final Map<Object, Integer> path) {
    final Optional<SchemaPruner> beneath = observed.flatMap(pruner -> pruner.descend(name));
    if (observed.isPresent() && beneath.isEmpty()) {
      // The source carried something here, but not a structure, so there is nothing to descend
      // into and nothing to keep.
      return Optional.empty();
    }
    final Object identity = element.getTypeIdentity();
    if (observed.isEmpty() && !withinNestingBound(type, identity, path)) {
      return Optional.empty();
    }
    path.merge(identity, 1, Integer::sum);
    try {
      final StructType derived = build(child, beneath, path);
      return SchemaPruner.retainsStructure(derived) ? Optional.of(derived) : Optional.empty();
    } finally {
      path.compute(identity, (key, count) -> count == null || count <= 1 ? null : count - 1);
    }
  }

  /**
   * Answers whether a type may be expanded again on the path that reached it. The bound counts
   * recurrences of the same type rather than absolute depth, which is what the nesting option has
   * always meant. A reference is not nested within a reference at all, which is what keeps the
   * assigner of an identifier carried by a reference out of the schema.
   */
  private boolean withinNestingBound(
      @Nonnull final FHIRDefinedType type,
      @Nonnull final Object identity,
      @Nonnull final Map<Object, Integer> path) {
    final int bound = FHIRDefinedType.REFERENCE.equals(type) ? 0 : maxNestingLevel;
    return path.getOrDefault(identity, 0) <= bound;
  }

  /**
   * Derives the metadata group beside a primitive, which carries the id and the extensions that the
   * primitive itself has nowhere to put.
   */
  @Nonnull
  private Optional<DataType> metadataGroupType(
      @Nonnull final DefinitionCanonicalStructure structure,
      @Nonnull final LayoutEntry entry,
      @Nonnull final Optional<SchemaPruner> observed,
      @Nonnull final Map<Object, Integer> path) {
    final Optional<SchemaPruner> beneath =
        observed.flatMap(pruner -> pruner.descend(entry.getName()));
    if (observed.isPresent() && beneath.isEmpty()) {
      return Optional.empty();
    }
    final MetadataGroupStructure group = structure.metadataGroup(entry.getName()).orElseThrow();
    final List<StructField> fields = new ArrayList<>();
    if (beneath.map(pruner -> pruner.retains(LayoutFields.METADATA_GROUP_ID)).orElse(true)) {
      fields.add(nullable(LayoutFields.METADATA_GROUP_ID, DataTypes.StringType));
    }
    if (beneath
        .map(pruner -> pruner.retains(LayoutFields.METADATA_GROUP_EXTENSION))
        .orElse(enableExtensions)) {
      extensionField(group, beneath, path).ifPresent(fields::add);
    }
    return fields.isEmpty()
        ? Optional.empty()
        : Optional.of(new StructType(fields.toArray(StructField[]::new)));
  }

  /** Derives the extension field of a metadata group, from the extension element of its node. */
  @Nonnull
  private Optional<StructField> extensionField(
      @Nonnull final MetadataGroupStructure group,
      @Nonnull final Optional<SchemaPruner> beneath,
      @Nonnull final Map<Object, Integer> path) {
    final Optional<ElementDefinition> element = group.getExtensionElement();
    final Optional<DefinitionCanonicalStructure> structure =
        group.extensionStructure(LayoutFields.METADATA_GROUP_EXTENSION);
    if (element.isEmpty() || structure.isEmpty()) {
      return Optional.empty();
    }
    return structureType(
            structure.orElseThrow(),
            element.orElseThrow(),
            element.orElseThrow().getFhirType().orElseThrow(),
            LayoutFields.METADATA_GROUP_EXTENSION,
            beneath,
            path)
        .map(
            derived ->
                nullable(
                    LayoutFields.METADATA_GROUP_EXTENSION,
                    repeated(element.orElseThrow(), derived)));
  }

  /** Wraps a type in an array where the element it describes may repeat. */
  @Nonnull
  private static DataType repeated(
      @Nonnull final ElementDefinition element, @Nonnull final DataType type) {
    return element.isRepeating() ? DataTypes.createArrayType(type, true) : type;
  }

  /**
   * Builds a field that is nullable and carries no metadata, which every field of this layout is:
   * nullability describes the data rather than the definitions, and a field that differed in it
   * between the two modes would make structures of the same elements incomparable.
   */
  @Nonnull
  private static StructField nullable(@Nonnull final String name, @Nonnull final DataType type) {
    return new StructField(name, type, true, Metadata.empty());
  }
}
