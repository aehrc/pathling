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

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Provides the functionality of the union operator within FHIRPath, i.e. {@code |}.
 *
 * <p>Merges two collections into a single collection, eliminating any duplicate values using
 * equality semantics. There is no expectation of order in the resulting collection.
 *
 * <p>Type compatibility is determined through FHIRPath type reconciliation. Collections with
 * compatible types are merged after type promotion (e.g., Date → DateTime, Integer → Decimal).
 *
 * <p>Equality semantics are determined by the collection's comparator. Types using default SQL
 * equality leverage Spark's native array operations, while types with custom equality (Quantity,
 * Coding, temporal types) use element-wise comparison. The array-level merge primitives are shared
 * with {@link CombineOperator} via {@link CombiningLogic}.
 *
 * <p>This operator is also reachable via the FHIRPath {@code union(other)} function, which is
 * desugared at parse time into the same AST used for the {@code |} operator. See the invocation
 * visitor for details.
 *
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhirpath/#union-collections">union</a>
 */
public class UnionOperator extends SameTypeBinaryOperator {

  @Nonnull
  @Override
  protected Collection handleOneEmpty(
      @Nonnull final Collection nonEmpty, @Nonnull final BinaryOperatorInput input) {
    final Column array = CombiningLogic.prepareArray(nonEmpty);
    final Column deduplicatedArray = CombiningLogic.dedupeArray(array, nonEmpty.getComparator());
    return nonEmpty.copyWithColumn(deduplicatedArray);
  }

  @Nonnull
  @Override
  protected Collection handleEquivalentTypes(
      @Nonnull final Collection left,
      @Nonnull final Collection right,
      @Nonnull final BinaryOperatorInput input) {
    final Column leftArray = CombiningLogic.prepareArray(left);
    final Column rightArray = CombiningLogic.prepareArray(right);
    final Column unionResult =
        CombiningLogic.unionArrays(leftArray, rightArray, left.getComparator());
    return left.copyWithColumn(unionResult);
  }

  @Nonnull
  @Override
  public String getOperatorName() {
    return "|";
  }
}
