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

package au.csiro.pathling.sql.misc;

import au.csiro.pathling.sql.udf.SqlFunction2;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark UDF to convert a Decimal type to a valid Decimal literal string with the optionally
 * provided scale.
 * <p>
 * If the scale is less than the number of decimal places in the Decimal struct, the function floors
 * the decimal value.
 * <p>
 * If the scale is negative, the function returns null.
 *
 * @author Piotr Szul
 */
public class DecimalToLiteral implements SqlFunction2<BigDecimal, Integer, String> {

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "decimal_to_literal";

  @Serial
  private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.StringType;
  }

  @Override
  @Nullable
  public String call(@Nullable final BigDecimal value, @Nullable final Integer scale) {
    if (value == null || (scale != null && scale < 0)) {
      return null;
    }
    return Optional.ofNullable(scale)
        .map(s -> value.setScale(s, RoundingMode.FLOOR))
        .orElse(value)
        .toString();
  }
}
