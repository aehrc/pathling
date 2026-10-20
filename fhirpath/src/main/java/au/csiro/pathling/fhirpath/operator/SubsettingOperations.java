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

import static org.apache.spark.sql.functions.get;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

/**
 * Operations for subsetting collections by index.
 *
 * @author John Grimes
 */
public class SubsettingOperations {

  private SubsettingOperations() {}

  /**
   * Returns a collection containing the element at the specified zero-based index, or an empty
   * collection if the index is empty or lies outside the bounds of the subject.
   *
   * @param subject The collection to index
   * @param index The index to use, which must be a singular Integer
   * @return A collection containing the element at the specified index, or an empty collection
   * @throws InvalidUserInputError if the index is not an Integer
   * @see <a href="https://hl7.org/fhirpath/#index-integer-collection">Indexer operation</a>
   */
  @FhirPathOperator(name = "[]")
  @Nonnull
  public static Collection index(
      @Nonnull final Collection subject, @Nonnull final Collection index) {
    if (index instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }
    if (!(index instanceof IntegerCollection)) {
      throw new InvalidUserInputError("Indexer operator requires an Integer index");
    }
    // A computed index such as `count() - 1` is a long, while `get` requires an int. An index
    // beyond the int range is out of bounds anyway, so try_cast turning it into null (and the
    // result into empty) is correct.
    final Column indexValue =
        index
            .asSingular("Indexer operator requires a singular index")
            .getColumnValue()
            .try_cast(DataTypes.IntegerType);
    return subject.map(
        rep ->
            rep.vectorize(
                // `get` is zero-based and returns null for any index outside the bounds of the
                // array, including a negative one.
                c -> get(c, indexValue),
                // A singular subject is a collection of one item, so only index 0 returns it.
                c -> when(indexValue.equalTo(0), c)));
  }
}
