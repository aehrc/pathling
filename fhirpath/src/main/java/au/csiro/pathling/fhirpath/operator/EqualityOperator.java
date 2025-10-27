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

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.Equatable.EqualityOperation;
import jakarta.annotation.Nonnull;
import java.util.function.BinaryOperator;
import org.apache.spark.sql.Column;

/**
 * Provides the functionality of the family of equality operators within FHIRPath, i.e. {@code =},
 * {@code !=}.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#equality">Equality</a>
 */
public class EqualityOperator extends SameTypeBinaryOperator {

  @Nonnull
  private final EqualityOperation type;

  /**
   * @param type The type of operator
   */
  public EqualityOperator(@Nonnull final EqualityOperation type) {
    this.type = type;
  }

  @Override
  @Nonnull
  protected Collection handleEquivalentTypes(@Nonnull final Collection leftCollection,
      @Nonnull final Collection rightCollection, @Nonnull final BinaryOperatorInput input) {

    // currently we only support equality for FHIRPath types
    if (leftCollection.getType().isEmpty() || rightCollection.getType().isEmpty()) {
      throw new UnsupportedFhirPathFeatureError("Unsupported equality for complex types");
    }

    // if types are compatible do element by element application of element comparator
    // We do actually use the equalTo and nonEqualTo methods here, rather than negating the
    // result of equalTo because this may be more efficient in some cases.
    final BinaryOperator<Column> elementComparator = type.bind(leftCollection.getComparator());
    final BinaryOperator<Column> arrayComparator = type.bind(
        leftCollection.getComparator().asArrayComparator());

    final ColumnRepresentation left = leftCollection.getColumn();
    final ColumnRepresentation right = rightCollection.getColumn();

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
                arrayComparator.apply(left.plural().getValue(), right.plural().getValue()));
    return BooleanCollection.build(new DefaultRepresentation(equalityResult));
  }

  @Override
  @Nonnull
  protected Collection handleNonEquivalentTypes(@Nonnull final Collection left,
      @Nonnull final Collection right, @Nonnull final BinaryOperatorInput input) {
    // for different types it's either dynamic null if any is null or false otherwise
    final Column equalityResult = when(
        left.getColumn().isEmpty().getValue().or(right.getColumn().isEmpty().getValue()),
        lit(null)
    ).otherwise(lit(type == EqualityOperation.NOT_EQUALS));
    return BooleanCollection.build(new DefaultRepresentation(equalityResult));
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return type.toString();
  }
}
