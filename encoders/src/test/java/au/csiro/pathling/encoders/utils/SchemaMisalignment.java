/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
package au.csiro.pathling.encoders.utils;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.transform_values;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.FhirEncoders;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Test support for constructing datasets whose schema is deliberately misaligned with the encoder
 * that decodes them.
 *
 * <p>The {@code encoders} module has no Delta dependency, so the two states that put a warehouse
 * out of alignment - a table migrated with {@code mergeSchema}, and a table written by a wider
 * encoder than the one reading it - are reproduced here by projection rather than by writing
 * storage. {@link #migratedSchema} models what a {@code mergeSchema} migration does to a schema,
 * and {@link #conformTo} presents a dataset under any nominated schema, resolving fields by name at
 * every depth.
 *
 * <p>The Delta-level form of the same scenario, against a real migrated table, lives in {@code
 * library-api}, which does have delta-spark.
 *
 * @author John Grimes
 */
public final class SchemaMisalignment {

  /** The open types the narrow encoder configures. */
  public static final Set<String> NARROW_OPEN_TYPES = FhirEncoders.STANDARD_OPEN_TYPES;

  /**
   * The narrow open types plus {@code Period} and {@code Quantity}. Those two are useful here
   * because their extension value fields sort into the middle of the extension struct rather than
   * the end, so enabling them shifts the position of every field after them.
   */
  public static final Set<String> WIDE_OPEN_TYPES =
      Stream.concat(NARROW_OPEN_TYPES.stream(), Stream.of("Period", "Quantity"))
          .collect(toUnmodifiableSet());

  private SchemaMisalignment() {}

  /**
   * Returns encoders configured with the narrow set of open types.
   *
   * @return the narrow encoders
   */
  @Nonnull
  public static FhirEncoders narrowEncoders() {
    return FhirEncoders.forR4()
        .withExtensionsEnabled(true)
        .withOpenTypes(NARROW_OPEN_TYPES)
        .getOrCreate();
  }

  /**
   * Returns encoders configured with the wide set of open types.
   *
   * @return the wide encoders
   */
  @Nonnull
  public static FhirEncoders wideEncoders() {
    return FhirEncoders.forR4()
        .withExtensionsEnabled(true)
        .withOpenTypes(WIDE_OPEN_TYPES)
        .getOrCreate();
  }

  /**
   * Presents the dataset under the given schema, resolving each target field against the dataset's
   * own schema by name at every level of nesting.
   *
   * <p>Fields the target names but the dataset does not carry become typed nulls; fields the
   * dataset carries but the target does not name are dropped; and the result is presented in the
   * target's field order. This projection stands in for storage in this module: combined with
   * {@link #migratedSchema} it produces the row layout a {@code mergeSchema} migration leaves
   * behind.
   *
   * @param dataset the dataset to project
   * @param target the schema to present it under
   * @return the projected dataset
   */
  @Nonnull
  public static Dataset<Row> conformTo(
      @Nonnull final Dataset<Row> dataset, @Nonnull final StructType target) {
    final StructType source = dataset.schema();
    final Column[] columns =
        Arrays.stream(target.fields())
            .map(
                field ->
                    findField(source, field.name())
                        .map(
                            sourceField ->
                                project(
                                    dataset.col(sourceField.name()),
                                    sourceField.dataType(),
                                    field.dataType()))
                        .orElseGet(() -> lit(null).cast(field.dataType()))
                        .alias(field.name()))
            .toArray(Column[]::new);
    return dataset.select(columns);
  }

  /**
   * Returns the schema that a {@code mergeSchema} append of {@code incoming} onto a table holding
   * {@code stored} produces: each struct keeps the fields it already had, in the order it had them,
   * with the fields only the incoming schema carries appended after them. This holds at every level
   * of nesting, which is what makes a migrated table's nested structs disagree with the encoder
   * that migrated it.
   *
   * @param stored the schema the table already holds
   * @param incoming the schema being appended
   * @return the merged schema
   */
  @Nonnull
  public static StructType migratedSchema(
      @Nonnull final StructType stored, @Nonnull final StructType incoming) {
    return (StructType) migrated(stored, incoming);
  }

  /**
   * Returns the schema with the field order of every struct reversed, at every level of nesting.
   * This is the pure reordering case: no field is added and none removed, so nothing but the order
   * can account for a difference in the decoded result.
   *
   * @param schema the schema to reverse
   * @return the reversed schema
   */
  @Nonnull
  public static StructType withReversedFields(@Nonnull final StructType schema) {
    return (StructType) reversed(schema);
  }

  /**
   * Returns the schema with the element struct of the named collection column replaced by the
   * result of applying the given operator to it. The column may be an array of structs, or a map
   * whose values are arrays of structs, which is the shape of the extension container.
   *
   * <p>This is how a test nominates an explicit field order, or an explicit field name, for one
   * nested struct without disturbing the rest of the schema.
   *
   * @param schema the schema to rewrite
   * @param columnName the name of the top-level column to rewrite within
   * @param operator the rewrite to apply to the element struct
   * @return the rewritten schema
   * @throws IllegalArgumentException if the named column is not a collection of structs
   */
  @Nonnull
  public static StructType mapElementStruct(
      @Nonnull final StructType schema,
      @Nonnull final String columnName,
      @Nonnull final UnaryOperator<StructType> operator) {
    final StructField field = schema.apply(columnName);
    final StructField[] fields = schema.fields().clone();
    fields[schema.fieldIndex(columnName)] =
        withDataType(field, mapElementStruct(field.dataType(), operator));
    return new StructType(fields);
  }

  /**
   * Returns the struct type found at the bottom of the named collection column, unwrapping arrays
   * and map values along the way.
   *
   * @param schema the schema to look in
   * @param columnName the name of the top-level column
   * @return the element struct type
   * @throws IllegalArgumentException if the named column is not a collection of structs
   */
  @Nonnull
  public static StructType elementStruct(
      @Nonnull final StructType schema, @Nonnull final String columnName) {
    final List<StructType> captured = new ArrayList<>();
    mapElementStruct(
        schema,
        columnName,
        element -> {
          captured.add(element);
          return element;
        });
    return captured.getFirst();
  }

  /**
   * Returns the given struct type with its fields in the nominated order.
   *
   * @param struct the struct type to reorder
   * @param fieldOrder the field names, which must be exactly the struct's own field names
   * @return the reordered struct type
   * @throws IllegalArgumentException if the nominated order is not a permutation of the struct's
   *     field names
   */
  @Nonnull
  public static StructType withFieldOrder(
      @Nonnull final StructType struct, @Nonnull final List<String> fieldOrder) {
    final Set<String> nominated = new HashSet<>(fieldOrder);
    final Set<String> present = new HashSet<>(Arrays.asList(struct.fieldNames()));
    if (!nominated.equals(present) || fieldOrder.size() != struct.fields().length) {
      throw new IllegalArgumentException(
          "The nominated field order must be a permutation of the struct's field names, but was "
              + fieldOrder
              + " for "
              + Arrays.toString(struct.fieldNames()));
    }
    return new StructType(fieldOrder.stream().map(struct::apply).toArray(StructField[]::new));
  }

  /**
   * Projects a column onto a target type, resolving struct fields by name at every level of
   * nesting.
   */
  @Nonnull
  private static Column project(
      @Nonnull final Column column,
      @Nonnull final DataType sourceType,
      @Nonnull final DataType targetType) {
    if (sourceType instanceof final StructType source
        && targetType instanceof final StructType target) {
      final Column[] fields =
          Arrays.stream(target.fields())
              .map(
                  field ->
                      findField(source, field.name())
                          .map(
                              sourceField ->
                                  project(
                                      column.getField(sourceField.name()),
                                      sourceField.dataType(),
                                      field.dataType()))
                          .orElseGet(() -> lit(null).cast(field.dataType()))
                          .alias(field.name()))
              .toArray(Column[]::new);
      // A struct built field by field is never null, so the null case has to be restored
      // explicitly. Without this, a null nested struct would decode as an empty composite rather
      // than as absent.
      return when(column.isNull(), lit(null).cast(target)).otherwise(struct(fields));
    }
    if (sourceType instanceof final ArrayType source
        && targetType instanceof final ArrayType target) {
      return transform(
          column, element -> project(element, source.elementType(), target.elementType()));
    }
    if (sourceType instanceof final MapType source && targetType instanceof final MapType target) {
      return transform_values(
          column, (key, value) -> project(value, source.valueType(), target.valueType()));
    }
    return column.cast(targetType);
  }

  @Nonnull
  private static DataType migrated(
      @Nonnull final DataType stored, @Nonnull final DataType incoming) {
    if (stored instanceof final StructType storedStruct
        && incoming instanceof final StructType incomingStruct) {
      final List<StructField> fields = new ArrayList<>();
      for (final StructField field : storedStruct.fields()) {
        fields.add(
            findField(incomingStruct, field.name())
                .map(
                    incomingField ->
                        withDataType(field, migrated(field.dataType(), incomingField.dataType())))
                .orElse(field));
      }
      for (final StructField field : incomingStruct.fields()) {
        if (findField(storedStruct, field.name()).isEmpty()) {
          // A field only the incoming schema carries is appended, which is what shifts the
          // positions of the fields already present.
          fields.add(field);
        }
      }
      return new StructType(fields.toArray(new StructField[0]));
    }
    if (stored instanceof final ArrayType storedArray
        && incoming instanceof final ArrayType incomingArray) {
      return new ArrayType(
          migrated(storedArray.elementType(), incomingArray.elementType()),
          storedArray.containsNull());
    }
    if (stored instanceof final MapType storedMap
        && incoming instanceof final MapType incomingMap) {
      return new MapType(
          storedMap.keyType(),
          migrated(storedMap.valueType(), incomingMap.valueType()),
          storedMap.valueContainsNull());
    }
    return stored;
  }

  @Nonnull
  private static DataType reversed(@Nonnull final DataType dataType) {
    if (dataType instanceof final StructType struct) {
      final List<StructField> fields = new ArrayList<>(Arrays.asList(struct.fields()));
      Collections.reverse(fields);
      return new StructType(
          fields.stream()
              .map(field -> withDataType(field, reversed(field.dataType())))
              .toArray(StructField[]::new));
    }
    if (dataType instanceof final ArrayType array) {
      return new ArrayType(reversed(array.elementType()), array.containsNull());
    }
    if (dataType instanceof final MapType map) {
      return new MapType(map.keyType(), reversed(map.valueType()), map.valueContainsNull());
    }
    return dataType;
  }

  @Nonnull
  private static DataType mapElementStruct(
      @Nonnull final DataType dataType, @Nonnull final UnaryOperator<StructType> operator) {
    if (dataType instanceof final ArrayType array) {
      return new ArrayType(mapElementStruct(array.elementType(), operator), array.containsNull());
    }
    if (dataType instanceof final MapType map) {
      return new MapType(
          map.keyType(), mapElementStruct(map.valueType(), operator), map.valueContainsNull());
    }
    if (dataType instanceof final StructType struct) {
      return operator.apply(struct);
    }
    throw new IllegalArgumentException("Not a collection of structs: " + dataType.simpleString());
  }

  /**
   * Returns the field with its data type replaced, preserving its name, nullability and metadata.
   */
  @Nonnull
  private static StructField withDataType(
      @Nonnull final StructField field, @Nonnull final DataType dataType) {
    return new StructField(field.name(), dataType, field.nullable(), field.metadata());
  }

  /** Looks a field up case-insensitively, matching the resolver Spark applies by default. */
  @Nonnull
  private static Optional<StructField> findField(
      @Nonnull final StructType struct, @Nonnull final String name) {
    return Arrays.stream(struct.fields())
        .filter(field -> field.name().equalsIgnoreCase(name))
        .findFirst();
  }
}
