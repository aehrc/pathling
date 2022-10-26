/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql.dates;

import au.csiro.pathling.sql.udf.SqlFunction2;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Base class for functions that compare temporal values.
 *
 * @author John Grimes
 */
public abstract class TemporalComparisonFunction<IntermediateType> implements
    SqlFunction2<String, String, Boolean> {

  private static final long serialVersionUID = 492467651418666881L;

  protected abstract Function<String, IntermediateType> parseEncodedValue();

  protected abstract BiFunction<IntermediateType, IntermediateType, Boolean> getOperationFunction();

  @Override
  public DataType getReturnType() {
    return DataTypes.BooleanType;
  }

  @Nullable
  @Override
  public Boolean call(@Nullable final String left, @Nullable final String right) throws Exception {
    if (left == null || right == null) {
      return null;
    }
    final IntermediateType parsedLeft = parseEncodedValue().apply(left);
    final IntermediateType parsedRight = parseEncodedValue().apply(right);
    return getOperationFunction().apply(parsedLeft, parsedRight);
  }

}
