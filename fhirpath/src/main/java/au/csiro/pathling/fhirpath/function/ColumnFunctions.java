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

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.column.ColumnCtx;
import au.csiro.pathling.fhirpath.validation.FhirpathFunction;
import au.csiro.pathling.fhirpath.validation.Numeric;
import au.csiro.pathling.fhirpath.validation.ReturnType;
import au.csiro.pathling.sql.misc.TemporalDifferenceFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

public class ColumnFunctions {

  @FhirpathFunction
  @ReturnType(FHIRDefinedType.BOOLEAN)
  public static Column not(@Nonnull final Column column) {
    return ColumnCtx.of(column).vectorize(
        c -> functions.when(c.isNotNull(), functions.transform(c, functions::not)),
        c -> functions.when(c.isNotNull(), functions.not(c))
    ).getValue();
  }


  //@FhirpathFunction
  @ReturnType(FHIRDefinedType.BOOLEAN)
  public static Column empty(@Nonnull final Column column) {
    return ColumnCtx.of(column).empty().getValue();
  }

  //@FhirpathFunction
  @ReturnType(FHIRDefinedType.INTEGER)
  public static Column count(@Nonnull final Column column) {
    return ColumnCtx.of(column).count().getValue();
  }

  //@FhirpathFunction
  public static Column first(@Nonnull final Column column) {
    // how to deal with nulls inside the expressioss?
    // technically should use filter to remove nulls, but that's expensive
    return ColumnCtx.of(column).first().getValue();
  }

  @FhirpathFunction
  public static Column last(@Nonnull final Column column) {
    // we need to use `element_at()` here are `getItem()` does not support column arguments
    // NOTE: `element_at()` is 1-indexed as opposed to `getItem()` which is 0-indexed
    return ColumnCtx.of(column).vectorize(
        c -> functions.when(c.isNull().or(functions.size(c).equalTo(0)), null)
            .otherwise(functions.element_at(c, functions.size(c))),
        Function.identity()
    ).getValue();
  }

  @FhirpathFunction
  public static Column singular(@Nonnull final Column column) {
    return ColumnCtx.of(column).singular().getValue();
  }

  @FhirpathFunction
  public static Column sum(@Nonnull @Numeric final Column column) {
    return ColumnCtx.of(column).aggregate(0, Column::plus).getValue();
  }
  //
  // @ReturnType(FHIRDefinedType.INTEGER)
  // public static Column until(@Nonnull final Column from, @Nonnull final Column to,
  //     @Nonnull final Column calendarDuration) {
  //   // the singularity is not strictly necessary here for from as we could lift it with transform
  //   return
  //       functions.call_udf(TemporalDifferenceFunction.FUNCTION_NAME, singular(from), singular(to),
  //           singular(calendarDuration));
  // }
}
