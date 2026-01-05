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

package au.csiro.pathling.fhirpath.comparison;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * An implementation of {@link ColumnComparator} that uses the standard Spark SQL comparison
 * operators.
 */
public class DefaultComparator implements ColumnComparator {

  private static final DefaultComparator INSTANCE = new DefaultComparator();

  /**
   * Gets the singleton instance of the {@link DefaultComparator}
   *
   * @return the singleton instance of {@link DefaultComparator}
   */
  @Nonnull
  public static DefaultComparator getInstance() {
    return INSTANCE;
  }

  protected DefaultComparator() {}

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
