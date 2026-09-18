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
package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * The recursive field-wise union of structure types, under a supplied canonical structure.
 *
 * <p>This is the single implementation serving both the reconciliation of collections whose SQL
 * shapes differ and the merging of divergent file schemas, so that the two can never disagree about
 * the type they produce.
 *
 * <p>The merge is commutative, associative and idempotent, which is what lets a merged schema be
 * reproduced no matter which operand or which file is seen first. That rests on the canonical
 * structure: the order of the result is a function of the set of field names present rather than of
 * the operands, because two subsequences of a total order do not determine that order.
 *
 * <p>Three decisions the specification leaves open are settled here.
 *
 * <ul>
 *   <li>Operands that disagree on the type of a field are rejected. The merge is a union of fields,
 *       not a lattice over types, and there is no type that faithfully carries both sides; choosing
 *       one would misrepresent the data on the other. That includes two decimals of differing
 *       precision, whose widening belongs where the values are prepared rather than here.
 *   <li>Fields the canonical structure does not order are appended after the ones it does, by name.
 *       The order has to come from somewhere, and anything taken from the operands — discovery
 *       order in particular — would make the result depend on merge order.
 *   <li>The null type is an identity, because it is how an absent element is typed. Merging it with
 *       anything yields the other side, and merging it with itself keeps it.
 * </ul>
 */
public abstract class StructureMerge {

  private StructureMerge() {}

  /**
   * Merges two structures into their recursive field-wise union, ordered canonically at every
   * level.
   *
   * @param left the first structure to merge
   * @param right the second structure to merge
   * @param canonical the canonical structure ordering the fields at the root of both operands
   * @return the merged structure
   * @throws IllegalArgumentException if the operands disagree on the type of a field
   */
  @Nonnull
  public static StructType merge(
      @Nonnull final StructType left,
      @Nonnull final StructType right,
      @Nonnull final CanonicalStructure canonical) {
    return mergeStructs(left, right, Optional.of(canonical), "");
  }

  /**
   * Merges any number of structures into their recursive field-wise union, ordered canonically at
   * every level.
   *
   * <p>The result does not depend on the order of the list, and a list of one is canonicalised in
   * the same way as a list of many.
   *
   * @param structures the structures to merge
   * @param canonical the canonical structure ordering the fields at the root of every operand
   * @return the merged structure
   * @throws IllegalArgumentException if the list is empty, or if two structures disagree on the
   *     type of a field
   */
  @Nonnull
  public static StructType merge(
      @Nonnull final List<StructType> structures, @Nonnull final CanonicalStructure canonical) {
    final StructType first =
        structures.stream()
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("Cannot merge an empty list of structures"));
    // The first structure is merged with itself, so that it is canonicalised even when it is the
    // only one. The merge is idempotent, so this does not change the result for a longer list.
    return structures.stream()
        .reduce(first, (accumulated, next) -> merge(accumulated, next, canonical));
  }

  /**
   * Merges two structures, taking the order of the result from the canonical structure at this
   * node.
   */
  @Nonnull
  private static StructType mergeStructs(
      @Nonnull final StructType left,
      @Nonnull final StructType right,
      @Nonnull final Optional<CanonicalStructure> here,
      @Nonnull final String path) {
    final Map<String, StructField> leftFields = byName(left);
    final Map<String, StructField> rightFields = byName(right);
    final StructField[] fields =
        order(union(leftFields.keySet(), rightFields.keySet()), here).stream()
            .map(
                name ->
                    mergeField(
                        name,
                        Optional.ofNullable(leftFields.get(name)),
                        Optional.ofNullable(rightFields.get(name)),
                        here,
                        path))
            .toArray(StructField[]::new);
    return new StructType(fields);
  }

  /**
   * Merges the two occurrences of a field, where a field carried by only one operand is kept and
   * made nullable, because the rows from the other operand have no value for it.
   */
  @Nonnull
  private static StructField mergeField(
      @Nonnull final String name,
      @Nonnull final Optional<StructField> left,
      @Nonnull final Optional<StructField> right,
      @Nonnull final Optional<CanonicalStructure> here,
      @Nonnull final String path) {
    final String fieldPath = path.isEmpty() ? name : path + "." + name;
    return left.flatMap(
            leftField ->
                right.map(
                    rightField ->
                        new StructField(
                            name,
                            mergeTypes(
                                leftField.dataType(), rightField.dataType(), here, name, fieldPath),
                            leftField.nullable() || rightField.nullable(),
                            agreedMetadata(leftField, rightField))))
        .orElseGet(
            () -> {
              final StructField only = left.or(() -> right).orElseThrow();
              return new StructField(
                  name,
                  canonicalise(only.dataType(), here, name, fieldPath),
                  true,
                  only.metadata());
            });
  }

  /**
   * Merges two types, descending into the canonical structure only where a structure is actually
   * reached. The canonical structure beneath a field is resolved here rather than by the caller,
   * because resolving it for a field the merge never descends into would force an expansion the
   * operands do not ask for.
   */
  @Nonnull
  private static DataType mergeTypes(
      @Nonnull final DataType left,
      @Nonnull final DataType right,
      @Nonnull final Optional<CanonicalStructure> parent,
      @Nonnull final String name,
      @Nonnull final String path) {
    if (left instanceof NullType) {
      return right instanceof NullType ? left : canonicalise(right, parent, name, path);
    }
    if (right instanceof NullType) {
      return canonicalise(left, parent, name, path);
    }
    if (left instanceof final StructType leftStruct
        && right instanceof final StructType rightStruct) {
      return mergeStructs(leftStruct, rightStruct, descend(parent, name), path);
    }
    if (left instanceof final ArrayType leftArray && right instanceof final ArrayType rightArray) {
      return DataTypes.createArrayType(
          mergeTypes(leftArray.elementType(), rightArray.elementType(), parent, name, path),
          leftArray.containsNull() || rightArray.containsNull());
    }
    if (left.equals(right)) {
      return left;
    }
    throw new IllegalArgumentException(
        "Cannot merge structures that disagree on the type of the field '"
            + path
            + "': "
            + left.simpleString()
            + " and "
            + right.simpleString());
  }

  /**
   * Orders a type canonically without merging it into anything, so that a structure reached through
   * one operand only is ordered the same way as one both operands carry.
   */
  @Nonnull
  private static DataType canonicalise(
      @Nonnull final DataType type,
      @Nonnull final Optional<CanonicalStructure> parent,
      @Nonnull final String name,
      @Nonnull final String path) {
    if (type instanceof final StructType struct) {
      final Optional<CanonicalStructure> here = descend(parent, name);
      final StructField[] fields =
          order(byName(struct).keySet(), here).stream()
              .map(struct::apply)
              .map(
                  field ->
                      new StructField(
                          field.name(),
                          canonicalise(
                              field.dataType(), here, field.name(), path + "." + field.name()),
                          field.nullable(),
                          field.metadata()))
              .toArray(StructField[]::new);
      return new StructType(fields);
    }
    if (type instanceof final ArrayType array) {
      return DataTypes.createArrayType(
          canonicalise(array.elementType(), parent, name, path), array.containsNull());
    }
    return type;
  }

  /**
   * Returns the canonical structure beneath the named field, which is empty both where the field
   * has no structure and where the canonical structure does not know the name.
   */
  @Nonnull
  private static Optional<CanonicalStructure> descend(
      @Nonnull final Optional<CanonicalStructure> parent, @Nonnull final String name) {
    return parent.flatMap(structure -> structure.field(name));
  }

  /**
   * Orders the names present at a node: the canonical ones first, in canonical order, then the rest
   * by name. Names the canonical structure orders but neither operand carries are omitted, so the
   * result is definition order restricted to the fields present.
   */
  @Nonnull
  private static List<String> order(
      @Nonnull final Set<String> present, @Nonnull final Optional<CanonicalStructure> here) {
    final List<String> canonical =
        here.map(CanonicalStructure::fieldOrder).orElseGet(List::of).stream()
            .filter(present::contains)
            .distinct()
            .toList();
    final Set<String> ordered = Set.copyOf(canonical);
    return Stream.concat(
            canonical.stream(), present.stream().filter(name -> !ordered.contains(name)).sorted())
        .toList();
  }

  @Nonnull
  private static Set<String> union(
      @Nonnull final Set<String> left, @Nonnull final Set<String> right) {
    final Set<String> union = new LinkedHashSet<>(left);
    union.addAll(right);
    return union;
  }

  @Nonnull
  private static Map<String, StructField> byName(@Nonnull final StructType structure) {
    return Stream.of(structure.fields())
        .collect(
            Collectors.toMap(
                StructField::name,
                Function.identity(),
                (first, second) -> first,
                LinkedHashMap::new));
  }

  /**
   * Returns the metadata the two occurrences of a field agree on, discarding it where they do not.
   * Keeping one side's metadata would make the merge depend on which operand came first.
   */
  @Nonnull
  private static Metadata agreedMetadata(
      @Nonnull final StructField left, @Nonnull final StructField right) {
    return left.metadata().equals(right.metadata()) ? left.metadata() : Metadata.empty();
  }
}
