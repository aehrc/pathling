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
import au.csiro.pathling.sql.types.FlexiDecimal;
import au.csiro.pathling.sql.udf.SqlFunction2;
import jakarta.annotation.Nullable;
import java.io.Serial;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

/**
 * Spark UDF that computes the canonical value of a stored quantity from its value and its unit
 * code, as a flexible decimal.
 *
 * <p>The value is taken as the text it is stored as, so that no digit is lost before
 * canonicalisation, and the canonical form is the one the previous layout's encoder stored beside
 * each quantity: {@link Ucum#getCanonicalValue} of the value and the code, whatever the system.
 * Until annotations are emitted, this is how the engine obtains the canonical form of a stored
 * quantity on either layout (FR-022).
 *
 * <p>Returns null if either input is null, if the value is not a decimal, or if the code cannot be
 * canonicalised.
 *
 * @see CanonicalQuantityCode
 */
public class CanonicalQuantityValue implements SqlFunction2<String, String, Row> {

  /** The name of this function when used within SQL. */
  public static final String FUNCTION_NAME = "canonical_quantity_value";

  @Serial private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return FlexiDecimal.DATA_TYPE;
  }

  @Override
  @Nullable
  public Row call(@Nullable final String value, @Nullable final String code) {
    return FlexiDecimal.toValue(Ucum.getCanonicalValue(CanonicalQuantityCode.parse(value), code));
  }
}
