/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.projection;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Utility methods for working with Spark SQL schemas.
 *
 * @author Piotr Szul
 */
public final class SchemaUtils {

  private SchemaUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Computes the maximum level of nesting in a StructType schema.
   *
   * <p>The maximum level of nesting is defined as the maximum count of fields with the same name
   * in any traversal path from the root StructType to any leaf field in the schema tree. The
   * traversal descends into arrays of structs as well as direct struct fields.</p>
   *
   * <p>For example, given a schema:</p>
   * <pre>
   * StructType(
   *   StructField("item", ArrayType(
   *     StructType(
   *       StructField("linkId", StringType),
   *       StructField("item", ArrayType(
   *         StructType(
   *           StructField("linkId", StringType)
   *         )
   *       ))
   *     )
   *   ))
   * )
   * </pre>
   *
   * <p>The maximum nesting level is 2, because the field name "item" appears twice in the path
   * from root to the nested "linkId" field.</p>
   *
   * @param schema the StructType schema to analyze
   * @return the maximum nesting level (minimum value is 1 for any non-empty schema)
   */
  public static int computeMaxNestingLevel(@Nonnull final StructType schema) {
    if (schema.fields().length == 0) {
      return 0;
    }
    return computeMaxNestingLevel(schema, new ArrayList<>());
  }

  /**
   * Recursively computes the maximum nesting level from a given DataType.
   *
   * @param dataType the data type to analyze
   * @param currentPath the current path of field names from the root
   * @return the maximum nesting level found in this subtree
   */
  private static int computeMaxNestingLevel(@Nonnull final DataType dataType,
      @Nonnull final List<String> currentPath) {
    if (dataType instanceof StructType structType) {
      return computeMaxNestingLevelInStruct(structType, currentPath);
    } else if (dataType instanceof ArrayType arrayType) {
      // Arrays of structs should be descended into
      return computeMaxNestingLevel(arrayType.elementType(), currentPath);
    } else {
      // Leaf node - compute the max count of any field name in the current path
      return computeMaxCountInPath(currentPath);
    }
  }

  /**
   * Computes the maximum nesting level within a StructType by recursively analyzing all fields.
   *
   * @param structType the struct type to analyze
   * @param currentPath the current path of field names from the root
   * @return the maximum nesting level found in this struct or its descendants
   */
  private static int computeMaxNestingLevelInStruct(@Nonnull final StructType structType,
      @Nonnull final List<String> currentPath) {
    int maxNesting = 0;

    for (StructField field : structType.fields()) {
      // Create a new path with this field added
      final List<String> newPath = new ArrayList<>(currentPath);
      newPath.add(field.name());

      // Recursively compute max nesting for this field's data type
      final int fieldMaxNesting = computeMaxNestingLevel(field.dataType(), newPath);
      maxNesting = Math.max(maxNesting, fieldMaxNesting);
    }

    return maxNesting;
  }

  /**
   * Computes the maximum count of any field name in the given path.
   *
   * <p>For example, if the path is ["item", "answer", "item", "linkId"], the maximum count is 2
   * (for "item"), so this returns 2.</p>
   *
   * @param path the list of field names representing a path from root to a leaf
   * @return the maximum count of any field name in the path
   */
  private static int computeMaxCountInPath(@Nonnull final List<String> path) {
    final Map<String, Integer> fieldCounts = new HashMap<>();

    // Count occurrences of each field name
    for (String fieldName : path) {
      fieldCounts.merge(fieldName, 1, Integer::sum);
    }

    // Return the maximum count
    return fieldCounts.values().stream()
        .mapToInt(Integer::intValue)
        .max()
        .orElse(0);
  }
}
