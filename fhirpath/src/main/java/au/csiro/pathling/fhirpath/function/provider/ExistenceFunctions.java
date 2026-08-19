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

package au.csiro.pathling.fhirpath.function.provider;

import static au.csiro.pathling.sql.SqlFunctions.let;
import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.functions;

/**
 * Contains functions for evaluating the existence of elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#existence">FHIRPath Specification -
 *     Existence</a>
 */
@SuppressWarnings("unused")
public class ExistenceFunctions {

  private ExistenceFunctions() {}

  /**
   * Returns {@code true} if the input collection has any elements (optionally filtered by the
   * criteria), and {@code false} otherwise. This is the opposite of {@code empty()}, and as such is
   * a shorthand for {@code empty().not()}. If the input collection is empty, the result is {@code
   * false}.
   *
   * <p>Using the optional criteria can be considered a shorthand for {@code
   * where(criteria).exists()}.
   *
   * @param input The input collection
   * @param criteria The criteria to apply to the input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#existscriteria--expression--boolean">FHIRPath
   *     Specification - exists</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static BooleanCollection exists(
      @Nonnull final Collection input, @Nullable final CollectionTransform criteria) {
    return BooleanLogicFunctions.not(
        empty(nonNull(criteria) ? FilteringAndProjectionFunctions.where(input, criteria) : input));
  }

  /**
   * Returns {@code true} if the input collection is empty and {@code false} otherwise.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#empty--boolean">FHIRPath Specification -
   *     empty</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static BooleanCollection empty(@Nonnull final Collection input) {
    return BooleanCollection.build(input.getColumn().isEmpty());
  }

  /**
   * Returns the integer count of the number of items in the input collection. Returns 0 when the
   * input collection is empty.
   *
   * @param input The input collection
   * @return An {@link IntegerCollection} containing the count
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#count--integer">FHIRPath Specification -
   *     count</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static IntegerCollection count(@Nonnull final Collection input) {
    return IntegerCollection.build(input.getColumn().count());
  }

  /**
   * Returns {@code true} if {@code criteria} evaluates to {@code true} for every element in the
   * input collection, and {@code false} otherwise. If the input collection is empty, the result is
   * {@code true}.
   *
   * <p>Implemented as a comparison between the number of elements for which {@code criteria} holds
   * and the total number of elements, which is equivalent to the spec definition and reuses {@link
   * FilteringAndProjectionFunctions#where} to get the "evaluates to true" semantics (excluding
   * elements for which the criteria is {@code false} or empty) for free. The input column is
   * materialised once with {@code let()} before being passed to both {@code where()} and the total
   * count, so a nondeterministic operand (e.g. a traced column) is not evaluated twice.
   *
   * @param input The input collection
   * @param criteria The criteria to evaluate for each element
   * @return A {@link BooleanCollection} containing the result
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#allcriteria--expression--boolean">FHIRPath
   *     Specification - all</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection all(
      @Nonnull final Collection input, @Nonnull final CollectionTransform criteria) {
    return BooleanCollection.build(
        new DefaultRepresentation(
            let(
                input.getColumn().getValue(),
                boundValue -> {
                  final Collection boundInput = input.copyWithColumn(boundValue);
                  final Collection matching =
                      FilteringAndProjectionFunctions.where(boundInput, criteria);
                  return matching
                      .getColumn()
                      .count()
                      .getValue()
                      .equalTo(boundInput.getColumn().count().getValue());
                })));
  }

  /**
   * Takes a collection of Boolean values and returns {@code true} if all the items are {@code
   * true}. If any items are {@code false}, the result is {@code false}. If the input is empty, the
   * result is {@code true}.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#alltrue--boolean">FHIRPath Specification
   *     - allTrue</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection allTrue(@Nonnull final Collection input) {
    return BooleanCollection.build(input.asBooleanPath().getColumn().allTrue());
  }

  /**
   * Takes a collection of Boolean values and returns {@code true} if any of the items are {@code
   * true}. If all the items are {@code false}, or if the input is empty, the result is {@code
   * false}.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#anytrue--boolean">FHIRPath Specification
   *     - anyTrue</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection anyTrue(@Nonnull final Collection input) {
    return BooleanCollection.build(input.asBooleanPath().getColumn().anyTrue());
  }

  /**
   * Takes a collection of Boolean values and returns {@code true} if all the items are {@code
   * false}. If any items are {@code true}, the result is {@code false}. If the input is empty, the
   * result is {@code true}.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#allfalse--boolean">FHIRPath Specification
   *     - allFalse</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection allFalse(@Nonnull final Collection input) {
    return BooleanCollection.build(input.asBooleanPath().getColumn().allFalse());
  }

  /**
   * Takes a collection of Boolean values and returns {@code true} if any of the items are {@code
   * false}. If all the items are {@code true}, or if the input is empty, the result is {@code
   * false}.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#anyfalse--boolean">FHIRPath Specification
   *     - anyFalse</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection anyFalse(@Nonnull final Collection input) {
    return BooleanCollection.build(input.asBooleanPath().getColumn().anyFalse());
  }

  /**
   * Returns {@code true} if all the items in the input collection are distinct. To determine
   * whether two items are distinct, the equals ({@code =}) operator is used. If the input
   * collection is empty, the result is {@code true}.
   *
   * <p>The input column is materialised once with {@code let()} before being passed to both the
   * deduplication logic and the total count, so a nondeterministic operand (e.g. a traced column)
   * is not evaluated twice.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#isdistinct--boolean">FHIRPath
   *     Specification - isDistinct</a>
   */
  @FhirPathFunction
  @Nonnull
  public static BooleanCollection isDistinct(@Nonnull final Collection input) {
    if (input.isEmpty()) {
      return BooleanCollection.build(new DefaultRepresentation(functions.lit(true)));
    }
    return BooleanCollection.build(
        new DefaultRepresentation(
            let(
                input.getColumn().getValue(),
                boundValue -> {
                  final Collection boundInput = input.copyWithColumn(boundValue);
                  final ColumnRepresentation distinctRepresentation =
                      new DefaultRepresentation(ExistenceLogic.distinct(boundInput));
                  return distinctRepresentation
                      .count()
                      .getValue()
                      .equalTo(boundInput.getColumn().count().getValue());
                })));
  }

  /**
   * Returns a collection containing only the unique items in the input collection. To determine
   * whether two items are the same, the equals ({@code =}) operator is used. If the input
   * collection is empty, the result is empty.
   *
   * <p>The order of elements in the result is not guaranteed to be preserved.
   *
   * @param input The input collection
   * @return A collection containing the distinct items
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#distinct--collection">FHIRPath
   *     Specification - distinct</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection distinct(@Nonnull final Collection input) {
    if (input.isEmpty()) {
      return input;
    }
    return input.copyWithColumn(ExistenceLogic.distinct(input));
  }
}
