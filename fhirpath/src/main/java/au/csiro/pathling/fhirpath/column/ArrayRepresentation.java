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

import java.util.function.Function;
import javax.annotation.Nonnull;

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.spark.sql.Column;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "of")
public class ArrayRepresentation extends ColumnRepresentation {

  Column value;

  @Override
  protected ColumnRepresentation copyOf(@Nonnull final Column newValue) {
    return ArrayRepresentation.of(newValue);
  }

  @Override
  @Nonnull
  public ArrayRepresentation vectorize(@Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return ArrayRepresentation.of(
        ValueFunctions.ifArray(value, arrayExpression::apply, singularExpression::apply));
  }

  @Override
  @Nonnull
  public ArrayRepresentation flatten() {
    return of(ValueFunctions.unnest(value));
  }

  @Override
  @Nonnull
  public ArrayRepresentation traverse(@Nonnull final String fieldName) {
    return of(ValueFunctions.unnest(value.getField(fieldName)));
  }
}
