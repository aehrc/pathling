/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import static au.csiro.pathling.fhirpath.FhirPathQuantity.FHIRPATH_CALENDAR_DURATION_SYSTEM_URI;
import static au.csiro.pathling.fhirpath.FhirPathQuantity.UCUM_SYSTEM_URI;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.FhirPathQuantity;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import java.io.Serial;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Spark UDF to convert a Quantity represented as a Row to a valid Quantity literal string.
 * <p>
 * UCUM units are quoted with single quotes, while time duration units are not quoted. For other
 * systems, the function returns null.
 * <p>
 * If the quantity is null, the function returns null.
 */
public class QuantityToLiteral implements SqlFunction1<Row, String> {

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "quantity_to_literal";

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
  public String call(@Nullable final Row row) {
    if (row == null) {
      return null;
    }

    try {
      // Decode the row into a FhirPathQuantity object using QuantityEncoding
      final FhirPathQuantity quantity = QuantityEncoding.decode(requireNonNull(row));

      final String system = quantity.getSystem();
      final String code = quantity.getCode();
      final String unit = quantity.getUnit();
      final String formattedValue = quantity.getValue().stripTrailingZeros().toPlainString();
      if (UCUM_SYSTEM_URI.equals(system)) {
        return formattedValue + " '" + code + "'";
      }
      if (FHIRPATH_CALENDAR_DURATION_SYSTEM_URI.equals(system)) {
        return formattedValue + " " + unit;
      }
      return null;
    } catch (final IllegalArgumentException e) {
      // Cannot decode quantity (null value, system, or code)
      return null;
    }
  }
}
