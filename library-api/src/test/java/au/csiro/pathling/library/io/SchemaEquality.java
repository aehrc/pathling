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

package au.csiro.pathling.library.io;

import au.csiro.pathling.utilities.AnnotationNames;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * The schema equality of spec SC-002, which the tests of a schema that is "the same" or "equal"
 * use: base schemas equal field for field, in order at every depth, ignoring nullability and field
 * metadata. Annotation fields are left out, since they are compared against the configuration, and
 * so is a group left with only annotation fields, at any depth.
 */
public final class SchemaEquality {

  private SchemaEquality() {}

  /**
   * Returns whether two schemas are the same.
   *
   * @param left one schema
   * @param right the other schema
   * @return whether they are the same
   */
  public static boolean same(@Nonnull final StructType left, @Nonnull final StructType right) {
    return base(left).equals(base(right));
  }

  /**
   * Returns whether two schema sets are the same: they name the same resource types, and the
   * schemas of each type are the same.
   *
   * @param left one schema set
   * @param right the other schema set
   * @return whether they are the same
   */
  public static boolean sameSet(
      @Nonnull final Map<String, StructType> left, @Nonnull final Map<String, StructType> right) {
    return left.keySet().equals(right.keySet())
        && left.keySet().stream().allMatch(type -> same(left.get(type), right.get(type)));
  }

  /**
   * Returns the base of a type: nullable throughout, without metadata, annotation fields or groups
   * left with only annotation fields at any depth.
   */
  @Nonnull
  private static DataType base(@Nonnull final DataType type) {
    if (type instanceof final StructType struct) {
      return new StructType(
          Arrays.stream(struct.fields())
              .filter(field -> !AnnotationNames.isAnnotation(field.name()))
              .flatMap(SchemaEquality::baseField)
              .toArray(StructField[]::new));
    } else if (type instanceof final ArrayType array) {
      return new ArrayType(base(array.elementType()), true);
    } else if (type instanceof final MapType map) {
      return new MapType(base(map.keyType()), base(map.valueType()), true);
    }
    return type;
  }

  /** Returns the base of a field, or nothing where it is a group emptied by taking its base. */
  @Nonnull
  private static Stream<StructField> baseField(@Nonnull final StructField field) {
    final DataType type = base(field.dataType());
    return emptied(field.dataType(), type)
        ? Stream.empty()
        : Stream.of(new StructField(field.name(), type, true, Metadata.empty()));
  }

  /**
   * Returns whether a group, or an array of groups, that held fields has none left in its base,
   * because every field it held was an annotation or a group emptied in turn.
   */
  private static boolean emptied(@Nonnull final DataType original, @Nonnull final DataType base) {
    if (original instanceof final ArrayType array && base instanceof final ArrayType baseArray) {
      return emptied(array.elementType(), baseArray.elementType());
    }
    return original instanceof final StructType struct
        && struct.fields().length > 0
        && base instanceof final StructType baseStruct
        && baseStruct.fields().length == 0;
  }
}
