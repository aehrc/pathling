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

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Implementation of Spark SQL comparator for quantities in the stored shape. It decodes each
 * operand into the structure the engine computes with, and compares the decoded quantities as
 * {@link QuantityComparator} does.
 *
 * <p>A quantity reached by traversal is held in the stored shape, so that it keeps its extensions.
 * The decoding is therefore applied where the quantities are compared, rather than where they are
 * traversed.
 *
 * @author Piotr Szul
 */
public class StoredQuantityComparator implements ColumnComparator, ElementWiseEquality {

  private static final StoredQuantityComparator INSTANCE = new StoredQuantityComparator();

  /**
   * Gets the singleton instance.
   *
   * @return the instance
   */
  @Nonnull
  public static StoredQuantityComparator getInstance() {
    return INSTANCE;
  }

  private StoredQuantityComparator() {}

  @Override
  @Nonnull
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return QuantityComparator.getInstance()
        .equalsTo(QuantityEncoding.decodeStored(left), QuantityEncoding.decodeStored(right));
  }

  @Override
  @Nonnull
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return QuantityComparator.getInstance()
        .lessThan(QuantityEncoding.decodeStored(left), QuantityEncoding.decodeStored(right));
  }

  @Override
  @Nonnull
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return QuantityComparator.getInstance()
        .lessThanOrEqual(QuantityEncoding.decodeStored(left), QuantityEncoding.decodeStored(right));
  }

  @Override
  @Nonnull
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return QuantityComparator.getInstance()
        .greaterThan(QuantityEncoding.decodeStored(left), QuantityEncoding.decodeStored(right));
  }

  @Override
  @Nonnull
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return QuantityComparator.getInstance()
        .greaterThanOrEqual(
            QuantityEncoding.decodeStored(left), QuantityEncoding.decodeStored(right));
  }
}
