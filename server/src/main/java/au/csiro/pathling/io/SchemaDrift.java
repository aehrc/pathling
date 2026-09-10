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

import jakarta.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Compares the schema produced by the current FHIR encoders against the schema of an existing Delta
 * table, in both directions. Nullability and column metadata differences are ignored throughout.
 *
 * <p>{@link #missingFieldPaths} reports paths the encoder emits that the table lacks. That
 * direction is migratable, by an additive schema evolution, and is the subject of the {@code
 * pathling.storage.schemaAutoMerge} policy.
 *
 * <p>{@link #excessFieldPaths} reports paths the table carries that the encoder does not emit. That
 * direction is not migratable: the columns cannot be reconstructed, and narrowing the table would
 * discard data. It exists so that startup can report the condition, and so that the write path can
 * recognise when it is safe to let Delta null-fill the columns it cannot supply. It is not a reason
 * to refuse service. Reading such a table back is made safe by the core-side decode fix in {@code
 * 048-name-based-nested-decode}, which resolves nested fields by name rather than by position; the
 * core Delta sink computes its own equivalent of this comparison, because the two modules are
 * released independently.
 *
 * @author John Grimes
 */
public final class SchemaDrift {

  private SchemaDrift() {}

  /**
   * Returns true if the source schema contains any field path (recursing through structs, arrays
   * and maps) that is absent from the target schema.
   *
   * @param source the candidate schema (typically the encoder output)
   * @param target the existing table schema
   * @return true if {@code source} introduces at least one field name not present in {@code target}
   */
  public static boolean hasMissingFields(
      @Nonnull final StructType source, @Nonnull final StructType target) {
    return !missingFieldPaths(source, target).isEmpty();
  }

  /**
   * Returns the field paths present in the source schema but absent from the target schema, in
   * lexicographic order.
   *
   * @param source the candidate schema (typically the encoder output)
   * @param target the existing table schema
   * @return the missing field paths, dot-separated
   */
  @Nonnull
  public static SortedSet<String> missingFieldPaths(
      @Nonnull final StructType source, @Nonnull final StructType target) {
    final SortedSet<String> missing = new TreeSet<>(collectFieldPaths(source));
    missing.removeAll(collectFieldPaths(target));
    return missing;
  }

  /**
   * Returns the field paths present in the target schema but absent from the source schema, in
   * lexicographic order. This is the reverse of {@link #missingFieldPaths}, and describes a table
   * that carries fields the encoder does not emit.
   *
   * @param source the candidate schema (typically the encoder output)
   * @param target the existing table schema
   * @return the excess field paths, dot-separated
   */
  @Nonnull
  public static SortedSet<String> excessFieldPaths(
      @Nonnull final StructType source, @Nonnull final StructType target) {
    return missingFieldPaths(target, source);
  }

  @Nonnull
  private static Set<String> collectFieldPaths(@Nonnull final StructType schema) {
    final Set<String> paths = new HashSet<>();
    collectFieldPaths(schema, "", paths);
    return paths;
  }

  private static void collectFieldPaths(
      @Nonnull final DataType type, @Nonnull final String prefix, @Nonnull final Set<String> out) {
    if (type instanceof final StructType struct) {
      for (final StructField field : struct.fields()) {
        final String path = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
        out.add(path);
        collectFieldPaths(field.dataType(), path, out);
      }
    } else if (type instanceof final ArrayType array) {
      collectFieldPaths(array.elementType(), prefix, out);
    } else if (type instanceof final MapType map) {
      collectFieldPaths(map.valueType(), prefix, out);
    }
  }
}
