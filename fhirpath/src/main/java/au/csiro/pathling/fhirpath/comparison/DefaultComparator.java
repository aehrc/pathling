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

package au.csiro.pathling.fhirpath.comparison;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.schema.LayoutFields;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * An implementation of {@link ColumnComparator} that uses the standard Spark SQL comparison
 * operators.
 */
public class DefaultComparator implements ColumnComparator {

  private static final DefaultComparator INSTANCE = new DefaultComparator();

  /**
   * Gets the singleton instance of the {@link DefaultComparator}.
   *
   * @return the singleton instance of {@link DefaultComparator}
   */
  @Nonnull
  public static DefaultComparator getInstance() {
    return INSTANCE;
  }

  protected DefaultComparator() {}

  /**
   * The annotation fields the engine recognises. They stay in a comparison key, because the value
   * determines them; any other annotation field is another producer's, and is left out of it
   * (decision 151).
   */
  @Nonnull
  public static final List<String> RECOGNISED_ANNOTATIONS =
      List.of(
          LayoutFields.START_ANNOTATION,
          LayoutFields.END_ANNOTATION,
          LayoutFields.NUMERIC_ANNOTATION,
          LayoutFields.CANONICAL_CODE_ANNOTATION,
          LayoutFields.CANONICAL_VALUE_EXACT_ANNOTATION);

  /**
   * Returns the deduplication of an array by the key of each element, keeping the first element of
   * each key, which is Spark's {@code array_distinct} where the key is the element itself (decision
   * 151).
   *
   * @param array the array to deduplicate
   * @return the deduplicated array
   */
  @Nonnull
  public static Column distinctByKey(@Nonnull final Column array) {
    return ColumnFunctions.distinctOnKey(array, RECOGNISED_ANNOTATIONS);
  }

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return left.equalTo(right);
  }

  @Nonnull
  @Override
  public Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.notEqual(right);
  }

  @Nonnull
  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return left.lt(right);
  }

  @Nonnull
  @Override
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.leq(right);
  }

  @Nonnull
  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return left.gt(right);
  }

  @Nonnull
  @Override
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return left.geq(right);
  }
}
