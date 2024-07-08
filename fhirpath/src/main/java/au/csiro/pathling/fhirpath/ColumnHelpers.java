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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.encoders.ValueFunctions;
import jakarta.annotation.Nonnull;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;

// TODO: Review of this class/functions are needed
public class ColumnHelpers {

  @Nonnull
  public static Column singular(@Nonnull final Column column) {

    return ValueFunctions.ifArray(column,
        c -> functions.when(functions.size(c).leq(1), c.getItem(0))
            .otherwise(functions.raise_error(
                functions.lit("Expected a single value, but found multiple values"))),
        x -> x);
  }

  @Nonnull
  public static Column aggregate(@Nonnull final Column column, @Nonnull final Object zeroValue,
      BiFunction<Column, Column, Column> aggregator) {

    return (column.expr().dataType() instanceof ArrayType)
           ? functions.when(column.isNull(), zeroValue)
               .otherwise(functions.aggregate(column, functions.lit(zeroValue), aggregator::apply))
           : functions.when(column.isNull(), zeroValue).otherwise(column);
    // this is OK because aggregator(zero, x) == x
  }

  public static Column map(@Nonnull final Column column,
      @Nonnull final Function<Column, Column> mapper) {
    return (column.expr().dataType() instanceof ArrayType)
           ? functions.when(column.isNull(), null)
               .otherwise(functions.transform(column, mapper::apply))
           : functions.when(column.isNotNull(), mapper.apply(column));
  }
}
