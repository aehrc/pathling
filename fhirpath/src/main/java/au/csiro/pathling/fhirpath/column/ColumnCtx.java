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

package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.encoders.ValueFunctions;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;

@Value(staticConstructor = "of")
public class ColumnCtx {

  Column value;

  @Nonnull
  public ColumnCtx vectorize(@Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return of(ValueFunctions.ifArray(value, arrayExpression::apply, singularExpression::apply));
  }

  @Nonnull
  public ColumnCtx vectorize(@Nonnull final Function<Column, Column> arrayExpression) {
    // the default implementation just wraps the element info array if needed
    return vectorize(arrayExpression,
        c -> arrayExpression.apply(functions.when(c.isNotNull(), functions.array(c))));
  }

  @Nonnull
  public ColumnCtx unnest() {
    return of(ValueFunctions.unnest(value));
  }
  
  @Nonnull
  public ColumnCtx traverse(@Nonnull final String fieldName) {
    return of(ValueFunctions.unnest(value.getField(fieldName)));
  }

  @Nonnull
  public ColumnCtx singular() {
    return vectorize(
        c -> functions.when(functions.size(c).leq(1), c.getItem(0))
            .otherwise(functions.raise_error(
                functions.lit("Expected a single value, but found multiple values"))),
        Function.identity()
    );
  }
  
  @Nonnull
  public ColumnCtx filter(Function<Column, Column> lambda) {
    return vectorize(
        c -> functions.filter(c, lambda::apply),
        c -> functions.when(c.isNotNull(), functions.when(lambda.apply(c), c))
    );
  }

  
  @Nonnull 
  public ColumnCtx transform(Function<Column, Column> lambda) {
    return vectorize(
        c -> functions.transform(c, lambda::apply),
        c -> functions.when(c.isNotNull(), lambda.apply(c))
    );
  }
  
  @Nonnull
  public ColumnCtx aggregate(@Nonnull final Object zeroValue,
      BiFunction<Column, Column, Column> aggregator) {

    return vectorize(
        c -> functions.when(c.isNull(), zeroValue)
            .otherwise(functions.aggregate(c, functions.lit(zeroValue), aggregator::apply)),
        c -> functions.when(c.isNull(), zeroValue).otherwise(c)
    );
    // this is OK because aggregator(zero, x) == x
  }


  @Nonnull
  public ColumnCtx first() {
    return vectorize(c -> c.getItem(0), Function.identity());
  }

  @Nonnull
  public ColumnCtx count() {
    return vectorize(
        c -> functions.when(c.isNull(), 0).otherwise(functions.size(c)),
        c -> functions.when(c.isNull(), 0).otherwise(1)
    );
  }

  @Nonnull
  public ColumnCtx empty() {
    return vectorize(
        c -> functions.when(c.isNotNull(), functions.size(c).equalTo(0)).otherwise(true),
        Column::isNull);
  }

}
