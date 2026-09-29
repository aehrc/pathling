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

package au.csiro.pathling.fhirpath.operator;

import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.array_union;
import static org.apache.spark.sql.functions.concat;

import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.DecodedRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.ColumnEquality;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.PrimitiveTypes;
import au.csiro.pathling.sql.SqlFunctions;
import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;

/**
 * The unification of operands that must share a type, and the array-level primitives used by the
 * FHIRPath combining operators.
 *
 * <p>{@link #unify(List)} is the one entry point through which every site that needs two or more
 * operands to share a type passes them (FR-056): equality, comparison, arithmetic, union, combine,
 * membership and choice traversal. It promotes the FHIR types of the operands and then unifies
 * their SQL shapes, in that order. The two steps answer different questions. Promotion is driven by
 * the definitions, and decides, for example, that an integer meets a decimal as a decimal. Shape
 * unification is structural: one FHIR type can be stored in a different shape at every path, and
 * the operands are projected by name into the merged shape of all of them.
 *
 * <p>A site that does not pass its operands through the entry point fails, where their shapes
 * differ, with a Spark analysis error that the engine cannot name.
 *
 * <p>The combining helpers are used by {@link UnionOperator}, which deduplicates, and {@link
 * CombineOperator}, which concatenates without deduplication. They operate on operands that have
 * already been unified.
 *
 * @author Piotr Szul
 */
@UtilityClass
public class CombiningLogic {

  /**
   * Unifies operands that must share a type: promotes their FHIR types to a common type where one
   * exists, then unifies their SQL shapes (FR-056).
   *
   * <p>Shape unification takes one of two forms.
   *
   * <ul>
   *   <li>Where every operand holds stored FHIR structures, each operand is projected by name into
   *       the merged structure of all of them, under the canonical order of the definitions
   *       (FR-057).
   *   <li>Where every operand holds structures the engine decoded from stored ones, the stored
   *       structures are reconciled, and decoded again. A combination of two FHIR operands
   *       therefore keeps the stored FHIR structures, and their extensions (decision 79). Where
   *       only some operands were decoded, a System value takes part, such as a literal. The FHIR
   *       operands are then taken as the System values they were decoded to, which already share
   *       the engine's structure.
   * </ul>
   *
   * <p>A primitive needs no shape unification here. Two decimals of different precision are given
   * one SQL type only where they are combined into one array, by {@link #prepareArray}, so that
   * arithmetic and comparison keep the precision of their operands.
   *
   * <p>Operands whose types cannot be reconciled are returned with their types unpromoted, and it
   * is for the calling site to decide what that means.
   *
   * @param operands the operands, in order
   * @return the unified operands, in the same order
   */
  @Nonnull
  public static List<Collection> unify(@Nonnull final List<Collection> operands) {
    return unifyShapes(promoteTypes(operands));
  }

  /**
   * Promotes the FHIR types of operands to the type of the first operand that every other operand
   * can be converted to. Where there is no such operand, the operands are returned unchanged.
   *
   * <p>For two operands this converts the right to the left where it can, and otherwise the left to
   * the right.
   *
   * @param operands the operands, in order
   * @return the promoted operands, in the same order
   */
  @Nonnull
  public static List<Collection> promoteTypes(@Nonnull final List<Collection> operands) {
    return operands.stream()
        .filter(
            target ->
                operands.stream()
                    .allMatch(operand -> operand == target || operand.convertibleTo(target)))
        .findFirst()
        .map(
            target ->
                operands.stream()
                    .map(operand -> operand == target ? operand : operand.castAs(target))
                    .toList())
        .orElse(operands);
  }

  /**
   * Unifies the SQL shapes of columns of one FHIR type, projecting each by name into the merged
   * type of all of them (FR-056). This is the core of {@link #unify(List)}, for a site that has
   * columns rather than collections.
   *
   * <p>Where no canonical structure is given, the type is a primitive, which has no structure to
   * reconcile, and the columns are returned as they are. The columns of a structure must all be
   * structures, or all be arrays of structures.
   *
   * @param columns the columns, in order
   * @param canonical the canonical structure of the type, where the type is a structure
   * @return the unified columns, in the same order
   */
  @Nonnull
  public static List<Column> unifyColumns(
      @Nonnull final List<Column> columns, @Nonnull final Optional<CanonicalStructure> canonical) {
    if (columns.size() < 2) {
      return columns;
    }
    return canonical
        .map(
            structure ->
                IntStream.range(0, columns.size())
                    .mapToObj(index -> ColumnFunctions.mergeCast(columns, index, structure))
                    .toList())
        .orElse(columns);
  }

  /**
   * Returns the canonical structure of an element, which orders the fields of the element's type,
   * or empty where the element is a primitive and has no structure.
   *
   * @param definition the definition of the element
   * @return the canonical structure of the element's type
   */
  @Nonnull
  public static Optional<CanonicalStructure> canonicalStructureOf(
      @Nonnull final ElementDefinition definition) {
    return definition
        .getFhirType()
        .filter(PrimitiveTypes::isStructure)
        .map(unused -> DefinitionCanonicalStructure.of(definition, false));
  }

  @Nonnull
  private static List<Collection> unifyShapes(@Nonnull final List<Collection> operands) {
    if (operands.size() < 2 || !typeEquivalent(operands)) {
      return operands;
    }
    final Optional<List<DecodedRepresentation>> decoded = decoded(operands);
    if (decoded.isPresent()) {
      return reconcileStored(operands, decoded.get());
    }
    if (operands.stream().allMatch(Collection::holdsStoredStructures)) {
      return reconcileStructures(operands);
    }
    return operands;
  }

  /** Projects every operand by name into the merged structure of all of them. */
  @Nonnull
  private static List<Collection> reconcileStructures(@Nonnull final List<Collection> operands) {
    // The operands are reconciled as arrays, because a singular structure and an array of them
    // have no merged type.
    return reconcile(
        operands,
        operands.stream().map(operand -> operand.getColumn().plural().getValue()).toList(),
        (index, unified) -> operands.get(index).copyWithColumn(unified));
  }

  /**
   * Reconciles the stored structures behind decoded operands, and decodes the reconciled structures
   * again, so that every operand keeps its stored structure.
   */
  @Nonnull
  private static List<Collection> reconcileStored(
      @Nonnull final List<Collection> operands,
      @Nonnull final List<DecodedRepresentation> decoded) {
    return reconcile(
        operands,
        decoded.stream().map(CombiningLogic::storedArray).toList(),
        (index, unified) -> rewrap(operands.get(index), unified, decoded.get(index).getDecoder()));
  }

  /**
   * Unifies one array column per operand under the canonical structure of the operands' type, and
   * rebuilds each operand from its unified column. Where the type has no structure, the operands
   * are returned as they are.
   */
  @Nonnull
  private static List<Collection> reconcile(
      @Nonnull final List<Collection> operands,
      @Nonnull final List<Column> columns,
      @Nonnull final BiFunction<Integer, Column, Collection> rebuild) {
    final Optional<CanonicalStructure> canonical = structureOfOperands(operands);
    if (canonical.isEmpty()) {
      return operands;
    }
    final List<Column> unified = unifyColumns(columns, canonical);
    return IntStream.range(0, operands.size())
        .mapToObj(index -> rebuild.apply(index, unified.get(index)))
        .toList();
  }

  /**
   * Returns the canonical structure of the operands' type, taken from the first operand that has
   * the definition of an element.
   */
  @Nonnull
  private static Optional<CanonicalStructure> structureOfOperands(
      @Nonnull final List<Collection> operands) {
    return operands.stream()
        .map(Collection::getDefinition)
        .flatMap(Optional::stream)
        .filter(ElementDefinition.class::isInstance)
        .map(ElementDefinition.class::cast)
        .findFirst()
        .flatMap(CombiningLogic::canonicalStructureOf);
  }

  private static boolean typeEquivalent(@Nonnull final List<Collection> operands) {
    final Collection first = operands.get(0);
    return operands.stream().allMatch(first::typeEquivalentWith);
  }

  /**
   * Returns the decoded representations of the operands, where every operand holds values the
   * engine decoded from stored structures.
   */
  @Nonnull
  private static Optional<List<DecodedRepresentation>> decoded(
      @Nonnull final List<Collection> operands) {
    return operands.stream()
            .allMatch(operand -> operand.getColumn() instanceof DecodedRepresentation)
        ? Optional.of(
            operands.stream().map(operand -> (DecodedRepresentation) operand.getColumn()).toList())
        : Optional.empty();
  }

  /**
   * Deduplicates the values of a unified collection, as the union of a collection with an empty
   * one.
   *
   * @param collection the unified collection
   * @return the collection, without duplicates
   */
  @Nonnull
  public static Collection dedupe(@Nonnull final Collection collection) {
    final ColumnEquality comparator = collection.getComparator();
    return decoded(List.of(collection))
        .map(
            decoded -> {
              final DecodedRepresentation only = decoded.get(0);
              final Column result =
                  SqlFunctions.arrayDistinctWithEquality(
                      storedArray(only), decodedEquality(comparator, only.getDecoder()));
              return rewrap(collection, result, only.getDecoder());
            })
        .orElseGet(
            () -> collection.copyWithColumn(dedupeArray(prepareArray(collection), comparator)));
  }

  /**
   * Merges two unified collections and deduplicates the result, as the FHIRPath union operator
   * does. Where both hold decoded values, the stored structures are merged, and compared by their
   * decoded values.
   *
   * @param left the left collection
   * @param right the right collection
   * @return the union of the two collections
   */
  @Nonnull
  public static Collection union(@Nonnull final Collection left, @Nonnull final Collection right) {
    final ColumnEquality comparator = left.getComparator();
    return decoded(List.of(left, right))
        .map(
            decoded -> {
              final UnaryOperator<Column> decoder = decoded.get(0).getDecoder();
              final Column result =
                  SqlFunctions.arrayUnionWithEquality(
                      storedArray(decoded.get(0)),
                      storedArray(decoded.get(1)),
                      decodedEquality(comparator, decoder));
              return rewrap(left, result, decoder);
            })
        .orElseGet(
            () ->
                left.copyWithColumn(
                    unionArrays(prepareArray(left), prepareArray(right), comparator)));
  }

  /**
   * Concatenates two unified collections without deduplication, as the FHIRPath {@code combine}
   * function does. Where both hold decoded values, the stored structures are concatenated.
   *
   * @param left the left collection
   * @param right the right collection
   * @return the combination of the two collections
   */
  @Nonnull
  public static Collection combine(
      @Nonnull final Collection left, @Nonnull final Collection right) {
    return decoded(List.of(left, right))
        .map(
            decoded ->
                rewrap(
                    left,
                    combineArrays(storedArray(decoded.get(0)), storedArray(decoded.get(1))),
                    decoded.get(0).getDecoder()))
        .orElseGet(
            () -> left.copyWithColumn(combineArrays(prepareArray(left), prepareArray(right))));
  }

  /** The equality of two stored structures, which is the equality of their decoded values. */
  @Nonnull
  private static BinaryOperator<Column> decodedEquality(
      @Nonnull final ColumnEquality comparator, @Nonnull final UnaryOperator<Column> decoder) {
    return (left, right) -> comparator.equalsTo(decoder.apply(left), decoder.apply(right));
  }

  /** Returns the stored structures behind a decoded representation, as an array. */
  @Nonnull
  private static Column storedArray(@Nonnull final DecodedRepresentation decoded) {
    return decoded.getStored().plural().getValue();
  }

  /** Builds a collection of the given stored structures, decoded as the template's were. */
  @Nonnull
  private static Collection rewrap(
      @Nonnull final Collection template,
      @Nonnull final Column stored,
      @Nonnull final UnaryOperator<Column> decoder) {
    return template.copyWith(new DecodedRepresentation(new DefaultRepresentation(stored), decoder));
  }

  /**
   * Extracts the array column from a unified collection in a form suitable for combining. A
   * primitive takes the SQL type every collection of its FHIRPath type shares, which reconciles two
   * decimals of different precision so that they can be held in one array.
   *
   * <p>This is done only where values are combined into one array. Equality, comparison and
   * arithmetic compute with the operands' own precision, as Spark does.
   *
   * @param collection the collection to extract the array column from
   * @return the array column ready for combining
   */
  @Nonnull
  public static Column prepareArray(@Nonnull final Collection collection) {
    return collection.withSharedSqlType().getColumn().plural().getValue();
  }

  /**
   * Deduplicates the values in an array using the appropriate equality strategy. Types that use
   * default SQL equality leverage Spark's {@code array_distinct}, while types with custom equality
   * (Quantity, Coding, temporal types) use element-wise comparison via {@link
   * SqlFunctions#arrayDistinctWithEquality}.
   *
   * @param arrayColumn the array column to deduplicate
   * @param comparator the equality comparator that defines element equality
   * @return the deduplicated array column
   */
  @Nonnull
  public static Column dedupeArray(
      @Nonnull final Column arrayColumn, @Nonnull final ColumnEquality comparator) {
    if (comparator.usesDefaultSqlEquality()) {
      return array_distinct(arrayColumn);
    }
    return SqlFunctions.arrayDistinctWithEquality(arrayColumn, comparator::equalsTo);
  }

  /**
   * Merges two arrays and deduplicates the result using the appropriate equality strategy. Types
   * that use default SQL equality leverage Spark's {@code array_union}, while types with custom
   * equality use element-wise comparison via {@link SqlFunctions#arrayUnionWithEquality}.
   *
   * @param leftArray the left array column
   * @param rightArray the right array column
   * @param comparator the equality comparator that defines element equality
   * @return the merged, deduplicated array column
   */
  @Nonnull
  public static Column unionArrays(
      @Nonnull final Column leftArray,
      @Nonnull final Column rightArray,
      @Nonnull final ColumnEquality comparator) {
    if (comparator.usesDefaultSqlEquality()) {
      return array_union(leftArray, rightArray);
    }
    return SqlFunctions.arrayUnionWithEquality(leftArray, rightArray, comparator::equalsTo);
  }

  /**
   * Concatenates two arrays without deduplication, preserving all duplicate values from both
   * operands. Used by the FHIRPath {@code combine(other)} function.
   *
   * @param leftArray the left array column
   * @param rightArray the right array column
   * @return the concatenated array column
   */
  @Nonnull
  public static Column combineArrays(
      @Nonnull final Column leftArray, @Nonnull final Column rightArray) {
    return concat(leftArray, rightArray);
  }
}
