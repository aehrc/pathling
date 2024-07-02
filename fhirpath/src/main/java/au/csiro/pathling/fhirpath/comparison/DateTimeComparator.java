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

package au.csiro.pathling.fhirpath.comparison;

import static org.apache.spark.sql.functions.callUDF;

import au.csiro.pathling.fhirpath.operator.ColumnComparator;
import au.csiro.pathling.fhirpath.operator.Comparable;
import au.csiro.pathling.fhirpath.operator.Comparable.ComparisonOperation;
import au.csiro.pathling.sql.dates.datetime.DateTimeEqualsFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeGreaterThanFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeGreaterThanOrEqualToFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeLessThanFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeLessThanOrEqualToFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Implementation of a Spark SQL comparator for the DateTime type.
 *
 * @author Piotr Szul
 */
public class DateTimeComparator implements ColumnComparator {

  private static final DateTimeComparator INSTANCE = new DateTimeComparator();

  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return callUDF(DateTimeEqualsFunction.FUNCTION_NAME, left, right);
  }

  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return callUDF(DateTimeLessThanFunction.FUNCTION_NAME, left, right);
  }

  @Override
  public Column lessThanOrEqual(final Column left, final Column right) {
    return callUDF(DateTimeLessThanOrEqualToFunction.FUNCTION_NAME, left, right);
  }

  @Override
  public Column greaterThan(final Column left, final Column right) {
    return callUDF(DateTimeGreaterThanFunction.FUNCTION_NAME, left, right);
  }

  @Override
  public Column greaterThanOrEqual(final Column left, final Column right) {
    return callUDF(DateTimeGreaterThanOrEqualToFunction.FUNCTION_NAME, left, right);
  }

  /**
   * Builds a comparison function for date and date/time like paths.
   *
   * @param source the path to build the comparison function for
   * @param operation the {@link ComparisonOperation} that should be built
   * @return a new {@link Function}
   */
  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(source, operation, INSTANCE);
  }
}
