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

import static au.csiro.pathling.utilities.SparkTypes.elementTypeOf;
import static au.csiro.pathling.utilities.SparkTypes.fieldOf;

import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * The rules about a primitive's stored metadata group that ingest, annotation and egress share, so
 * that they cannot disagree on which fields are groups or what a group holds.
 */
final class MetadataGroups {

  private MetadataGroups() {}

  /**
   * Returns whether a metadata group has the outer shape FHIR gives it: an object beside a singular
   * primitive, and an array of objects beside a repeating one.
   *
   * @param repeating whether the primitive the group accompanies repeats
   * @param type the type the group is carried as
   * @return true where the group has that shape
   */
  static boolean isGroupShape(final boolean repeating, @Nonnull final DataType type) {
    return repeating
        ? type instanceof final ArrayType array && array.elementType() instanceof StructType
        : type instanceof StructType;
  }

  /**
   * Returns whether a stored metadata group, or an array of them, has a field for an id or for
   * extensions, which a group the annotation step added for its annotations alone has not.
   *
   * @param type the type the group is stored as
   * @return true where the group has either field
   */
  static boolean holdsMetadata(@Nonnull final DataType type) {
    return elementTypeOf(type) instanceof final StructType group
        && LayoutFields.METADATA_GROUP_FIELDS.stream()
            .anyMatch(name -> fieldOf(group, name).isPresent());
  }

  /**
   * Returns whether a field of a stored metadata group has content: an id that is not null, or an
   * extension array holding an extension. An empty extension array, or one holding only nulls, has
   * none, so a group holding nothing else is not written (FR-019).
   *
   * @param name the name of the field, {@code id} or {@code extension}
   * @param field the field's value
   * @return the condition, which is not true where the field is null
   */
  @Nonnull
  static Column hasContent(@Nonnull final String name, @Nonnull final Column field) {
    return LayoutFields.METADATA_GROUP_EXTENSION.equals(name)
        ? functions.exists(field, Column::isNotNull)
        : field.isNotNull();
  }

  /**
   * Returns the structure of the extensions in the metadata group of a primitive.
   *
   * @param node the canonical structure of the node carrying the primitive
   * @param primitive the primitive the group accompanies
   * @return the structure, or empty where the node has no group for the primitive
   */
  @Nonnull
  static Optional<DefinitionCanonicalStructure> extensionStructure(
      @Nonnull final DefinitionCanonicalStructure node, @Nonnull final LayoutEntry primitive) {
    return node.metadataGroup(LayoutFields.metadataGroupName(primitive.getName()))
        .flatMap(group -> group.extensionStructure(LayoutFields.METADATA_GROUP_EXTENSION));
  }
}
