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
package au.csiro.pathling.io;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Puts a schema in this layout into canonical field order at every depth, the order an inferred
 * load gives (FR-027). It works on the schema alone, so no data is read and no job is run.
 *
 * <p>The order is the order of {@link DefinitionCanonicalStructure}: the resource type first, the
 * types of a choice in place, each metadata group after its primitive, and each annotation in the
 * place the annotation step puts it. A field keeps its nullability and its metadata, and an array
 * keeps whether it holds nulls. A field the definitions do not describe is dropped, as a read with
 * the schema drops it. A field whose type or cardinality contradicts the definitions is refused.
 */
final class SchemaCanonicalisation {

  @Nonnull private final String resourceType;

  private SchemaCanonicalisation(@Nonnull final String resourceType) {
    this.resourceType = resourceType;
  }

  /**
   * Returns a schema in canonical field order at every depth.
   *
   * @param definitions the definitions the canonical order is taken from
   * @param resourceType the type of the resources the schema describes
   * @param schema the schema, in this layout
   * @return the schema in canonical order
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type, or if
   *     a field contradicts the definitions in its type or cardinality, naming the type and the
   *     path of the field
   */
  @Nonnull
  static StructType canonicalSchema(
      @Nonnull final DefinitionContext definitions,
      @Nonnull final String resourceType,
      @Nonnull final StructType schema) {
    StorableResourceType.require(definitions, resourceType);
    return new SchemaCanonicalisation(resourceType)
        .described(DefinitionCanonicalStructure.forResource(definitions, resourceType), schema, "");
  }

  /** Orders a structure the definitions describe, checking each field against its entry. */
  @Nonnull
  private StructType described(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType schema,
      @Nonnull final String path) {
    final Map<String, StructField> given = byName(schema);
    return DataTypes.createStructType(
        node.entries().stream()
            .filter(entry -> given.containsKey(entry.getName()))
            .map(entry -> describedField(node, entry, given.get(entry.getName()), path))
            .toList());
  }

  @Nonnull
  private StructField describedField(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final StructField field,
      @Nonnull final String parentPath) {
    final String path = parentPath.isEmpty() ? field.name() : parentPath + "." + field.name();
    final DataType type;
    if (entry.isElement()) {
      requireCardinality(entry.isRepeating(), field.dataType(), path);
      type = mapElements(field.dataType(), value -> elementType(node, entry, value, path));
    } else if (entry.isMetadataGroup()) {
      if (!MetadataGroups.isGroupShape(entry.isRepeating(), field.dataType())) {
        throw contradiction(path, field.dataType());
      }
      final Optional<CanonicalStructure> group =
          node.metadataGroup(entry.getName()).map(CanonicalStructure.class::cast);
      type =
          mapElements(
              field.dataType(),
              value -> ordered(group, requireGroupFields((StructType) value, path), path));
    } else if (entry.isResourceType()) {
      if (!DataTypes.StringType.equals(field.dataType())) {
        throw contradiction(path, field.dataType());
      }
      type = field.dataType();
    } else {
      // An annotation is ordered within where it is a structure.
      final Optional<CanonicalStructure> annotation = node.field(entry.getName());
      type = ordered(annotation, field.dataType(), path);
    }
    return new StructField(field.name(), type, field.nullable(), field.metadata());
  }

  /** Checks the value of an element against its definition, ordering it where it is complex. */
  @Nonnull
  private DataType elementType(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType value,
      @Nonnull final String path) {
    final Optional<DataType> stored =
        entry
            .primitiveType()
            .map(
                primitive ->
                    PrimitiveConverters.forType(primitive)
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "No converter for primitive type: " + primitive))
                        .getStored());
    if (stored.isPresent()) {
      if (!stored.get().equals(value)) {
        throw contradiction(path, value);
      }
      return value;
    }
    if (value instanceof final StructType structure) {
      return node.elementStructure(entry)
          .<DataType>map(child -> described(child, structure, path))
          .orElse(structure);
    }
    throw contradiction(path, value);
  }

  /**
   * Orders a structure the definitions describe only by name, such as a metadata group or a
   * structured annotation, descending into a described structure wherever one is reached. Where no
   * order is known, the structure is kept as it is given.
   */
  @Nonnull
  private DataType ordered(
      @Nonnull final Optional<CanonicalStructure> here,
      @Nonnull final DataType type,
      @Nonnull final String path) {
    if (type instanceof final ArrayType array) {
      return DataTypes.createArrayType(
          ordered(here, array.elementType(), path), array.containsNull());
    }
    if (!(type instanceof final StructType structure) || here.isEmpty()) {
      return type;
    }
    final CanonicalStructure structureHere = here.get();
    if (structureHere instanceof final DefinitionCanonicalStructure node) {
      return described(node, structure, path);
    }
    final Map<String, StructField> given = byName(structure);
    return DataTypes.createStructType(
        structureHere.fieldOrder().stream()
            .filter(given::containsKey)
            .map(given::get)
            .map(
                field ->
                    new StructField(
                        field.name(),
                        ordered(
                            structureHere.field(field.name()),
                            field.dataType(),
                            path + "." + field.name()),
                        field.nullable(),
                        field.metadata()))
            .toList());
  }

  /**
   * Refuses a metadata group whose id is not a string or whose extensions are not an array. The
   * fields of each extension are checked as the group is ordered.
   */
  @Nonnull
  private StructType requireGroupFields(
      @Nonnull final StructType group, @Nonnull final String path) {
    final StructField id = byName(group).get(LayoutFields.METADATA_GROUP_ID);
    if (id != null && !DataTypes.StringType.equals(id.dataType())) {
      throw contradiction(path + "." + id.name(), id.dataType());
    }
    final StructField extension = byName(group).get(LayoutFields.METADATA_GROUP_EXTENSION);
    if (extension != null && !(extension.dataType() instanceof ArrayType)) {
      throw contradiction(path + "." + extension.name(), extension.dataType());
    }
    return group;
  }

  /** Refuses a field held as an array where its element does not repeat, or the reverse. */
  private void requireCardinality(
      final boolean repeating, @Nonnull final DataType type, @Nonnull final String path) {
    if (repeating != type instanceof ArrayType) {
      throw contradiction(path, type);
    }
  }

  @Nonnull
  private IllegalArgumentException contradiction(
      @Nonnull final String path, @Nonnull final DataType type) {
    return new IllegalArgumentException(
        "The schema of "
            + resourceType
            + " contradicts the definitions at "
            + path
            + ", which it gives as "
            + type.simpleString());
  }

  /** Applies a function to a type, or to the elements of an array, keeping the array's nulls. */
  @Nonnull
  private static DataType mapElements(
      @Nonnull final DataType type, @Nonnull final Function<DataType, DataType> function) {
    return type instanceof final ArrayType array
        ? DataTypes.createArrayType(function.apply(array.elementType()), array.containsNull())
        : function.apply(type);
  }

  @Nonnull
  private static Map<String, StructField> byName(@Nonnull final StructType structure) {
    return Stream.of(structure.fields())
        .collect(
            Collectors.toMap(StructField::name, Function.identity(), (first, second) -> first));
  }
}
