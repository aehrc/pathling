/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Merges the left and right operands into a single collection.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#combine">combine</a>
 */
public class CombineOperator implements BinaryOperator {

  private static final String NAME = "combine";

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {
    final Collection left = input.getLeft();
    final Collection right = input.getRight();

    // TODO: check the condition
    checkUserInput(left.getType().equals(right.getType()),
        "Collection must have the same type");
    // and also need to

    // if at least one is not empty then the result is of the type of the non-empty one and we can cast the other one to that type

    // just handle empty collections
    final Collection resultCollection = left instanceof EmptyCollection
                                        ? right
                                        : left;

    final Column[] concatColumns = Stream.of(left, right)
        .filter(c -> !(c instanceof EmptyCollection))
        .map(Collection::getColumn)
        .map(ColumnRepresentation::plural)
        .map(ColumnRepresentation::getValue)
        .toArray(Column[]::new);

    return resultCollection.copyWith(
        new DefaultRepresentation(
            functions.concat(concatColumns)
        ));
  }

  @Override
  @Nonnull
  public String getOperatorName() {
    return NAME;
  }


}
