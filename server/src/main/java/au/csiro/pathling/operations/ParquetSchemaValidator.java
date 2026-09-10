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

package au.csiro.pathling.operations;

import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Validates that a Spark result schema can be written to Parquet, rejecting schemas that contain an
 * unresolved ({@code NullType}, displayed as "VOID") type with a clear, user-correctable error.
 *
 * <p>Spark's Parquet writer rejects a {@code NullType} anywhere in the schema, not just at the top
 * level (for example, an {@code array()} literal produces an array of {@code NullType}). The walk
 * therefore recurses into structs, arrays, and maps and reports every offending field path in a
 * single message.
 *
 * @author John Grimes
 */
public final class ParquetSchemaValidator {

  private ParquetSchemaValidator() {
    // Utility class; not instantiable.
  }

  /**
   * Validates that the given schema contains no unresolved (VOID) types anywhere in its structure.
   *
   * @param schema the result schema to validate
   * @throws InvalidUserInputError if the schema contains one or more {@code NullType} fields; the
   *     message names every offending field path and suggests both remediations
   */
  public static void validateSchemaForParquet(@Nonnull final StructType schema) {
    final List<String> voidPaths = new ArrayList<>();
    walkStruct(schema, "", voidPaths);

    if (!voidPaths.isEmpty()) {
      final String columns =
          voidPaths.stream().map(path -> "'" + path + "'").collect(Collectors.joining(", "));
      throw new InvalidUserInputError(
          "The result contains column(s) with an unresolved (VOID) type that cannot be written to "
              + "Parquet: "
              + columns
              + ". Add an explicit CAST to the query or view (e.g. CAST(column AS STRING)), or "
              + "choose a different output format.");
    }
  }

  /** Recurses over the fields of a struct, extending the accumulated field path for each. */
  private static void walkStruct(
      @Nonnull final StructType struct,
      @Nonnull final String path,
      @Nonnull final List<String> voidPaths) {
    for (final StructField field : struct.fields()) {
      final String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
      walkType(field.dataType(), fieldPath, voidPaths);
    }
  }

  /** Recurses over a single data type, recording the path when a {@code NullType} is reached. */
  private static void walkType(
      @Nonnull final DataType dataType,
      @Nonnull final String path,
      @Nonnull final List<String> voidPaths) {
    if (dataType instanceof NullType) {
      voidPaths.add(path);
    } else if (dataType instanceof final StructType struct) {
      walkStruct(struct, path, voidPaths);
    } else if (dataType instanceof final ArrayType array) {
      walkType(array.elementType(), path + "[]", voidPaths);
    } else if (dataType instanceof final MapType map) {
      walkType(map.keyType(), path + ".key", voidPaths);
      walkType(map.valueType(), path + ".value", voidPaths);
    }
  }
}
