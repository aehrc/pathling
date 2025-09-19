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

package au.csiro.pathling.fhirpath.operator;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.Equatable.EqualityOperation;
import jakarta.annotation.Nonnull;
import java.util.function.BinaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.jetbrains.annotations.Contract;

/**
 * Provides the functionality of the family of equality operators within FHIRPath, i.e. {@code =},
 * {@code !=}.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#equality">Equality</a>
 */
public class EqualityOperator implements FhirPathBinaryOperator {

  @Nonnull
  private final EqualityOperation type;

  /**
   * @param type The type of operator
   */
  public EqualityOperator(@Nonnull final EqualityOperation type) {
    this.type = type;
  }

  /**
   * Wraps a comparator function so that it can be applied to two array columns, producing a
   * comparison results. The final result is true if all elements in the arrays compare as true
   * (using the provided comparator), and false if any element compares as false. If the arrays
   * differ in size, the result is false. This assumes that both arrays non-null .
   *
   * @param comparator the comparator function to wrap
   * @return a comparator function that can be applied to two array columns
   */
  @Nonnull
  @Contract(pure = true)
  private static BinaryOperator<Column> asArrayComparator(
      @Nonnull final BinaryOperator<Column> comparator, final boolean defaultNonMatch) {
    return (left, right) -> {
      final Column zip = functions.zip_with(left, right, comparator::apply);
      // Check if all elements in the zipped array are true in case of equality
      // or any is true in case of inequality
      final Column arrayResult = defaultNonMatch
                                 ? functions.exists(zip, c -> c)
                                 : functions.forall(zip, c -> c);
      // If the arrays are of different sizes, return false.
      return functions.when(
          functions.size(left).equalTo(functions.size(right)),
          // this is to handle the case where some elements cannot be compared
          // e.g. temporal values with different precisions
          // in which case arrayResult may be null
          functions.coalesce(arrayResult, lit(defaultNonMatch))
      ).otherwise(functions.lit(defaultNonMatch));
    };
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {
    final Collection leftCollection = input.left();
    final Collection rightCollection = input.right();

    // If either operand is an EmptyCollection, return an EmptyCollection.
    if (leftCollection instanceof EmptyCollection || rightCollection instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    final ColumnRepresentation left = leftCollection.getColumn();
    final ColumnRepresentation right = rightCollection.getColumn();

    if (!leftCollection.isComparableTo(rightCollection)) {
      // for different types it's either dynamic null if any is null or false otherwise
      final Column equalityResult = when(
          left.isEmpty().getValue().or(right.isEmpty().getValue()), lit(null))
          .otherwise(lit(type == EqualityOperation.NOT_EQUALS));
      return BooleanCollection.build(new DefaultRepresentation(equalityResult));
    }

    // if types are compatible do element by element application of element comparator
    // We do actually use the equalTo and nonEqualTo methods here, rather than negating the
    // result of equalTo because this may be more efficient in some cases.
    final BinaryOperator<Column> elementComparator = type.bind(leftCollection.getComparator());

    final Column equalityResult =
        when(
            left.isEmpty().getValue().or(right.isEmpty().getValue()),
            lit(null))
            .when(
                left.count().getValue().equalTo(lit(1))
                    .and(right.count().getValue().equalTo(lit(1))),
                // this works because we know both sides are singular (count == 1)
                elementComparator.apply(left.singular().getValue(), right.singular().getValue()))
            .otherwise(
                // this works because we know that both sides is plural (count > 1)
                asArrayComparator(elementComparator, type == EqualityOperation.NOT_EQUALS)
                    .apply(left.plural().getValue(), right.plural().getValue()));
    return BooleanCollection.build(new DefaultRepresentation(equalityResult));
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return type.toString();
  }
}
