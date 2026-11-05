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

package au.csiro.pathling.sql.misc;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.sql.udf.SqlFunction2;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.math.BigDecimal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark UDF that computes the canonical unit code of a stored quantity from its value and its unit
 * code.
 *
 * <p>The canonical form is the one the previous layout's encoder stored beside each quantity:
 * {@link Ucum#getCanonicalCode} of the value and the code, whatever the system. The value takes
 * part because some conversions, such as those between temperature scales, are not purely
 * multiplicative.
 *
 * <p>Returns null if either input is null, if the value is not a decimal, or if the code cannot be
 * canonicalised.
 *
 * @see CanonicalQuantityValue
 */
public class CanonicalQuantityCode implements SqlFunction2<String, String, String> {

  /** The name of this function when used within SQL. */
  public static final String FUNCTION_NAME = "canonical_quantity_code";

  @Serial private static final long serialVersionUID = 1L;

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
  public String call(@Nullable final String value, @Nullable final String code) {
    return Ucum.getCanonicalCode(parse(value), code);
  }

  /**
   * Parses the stored text of a decimal.
   *
   * @param value the stored text
   * @return the decimal, or null if the text is null or is not a decimal
   */
  @Nullable
  static BigDecimal parse(@Nullable final String value) {
    if (value == null) {
      return null;
    }
    try {
      return new BigDecimal(value.trim());
    } catch (final NumberFormatException e) {
      return null;
    }
  }
}
