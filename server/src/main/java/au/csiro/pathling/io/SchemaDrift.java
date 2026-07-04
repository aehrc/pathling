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
 * Detects schema drift between the schema produced by the current FHIR encoders and the schema of
 * an existing Delta table. Only field paths present in the source but absent from the target count
 * as drift; nullability and column metadata differences are ignored, and extra target-only fields
 * are tolerated.
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
