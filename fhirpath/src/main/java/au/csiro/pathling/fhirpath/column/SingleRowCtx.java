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

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import javax.annotation.Nonnull;
import java.util.function.Function;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "of")
public class SingleRowCtx extends ColumnCtx {

  Column value;

  @Override
  protected ColumnCtx copyOf(@Nonnull final Column newValue) {
    return SingleRowCtx.of(newValue);
  }

  @Override
  @Nonnull
  public SingleRowCtx vectorize(@Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return SingleRowCtx.of(singularExpression.apply(value));
  }

  @Override
  @Nonnull
  public ColumnCtx flatten() {
    // TODO: ???? What should it be
    throw new UnsupportedOperationException("Cannot flatten a single row");
  }

  @Override
  @Nonnull
  public ColumnCtx traverse(@Nonnull final String fieldName) {
    return StdColumnCtx.of(
        functions.when(value.isNotNull(), functions.col(fieldName))
    );
  }
}
