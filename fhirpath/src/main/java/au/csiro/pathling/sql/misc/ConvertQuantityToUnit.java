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

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.FhirPathQuantity;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.udf.SqlFunction2;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.util.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

/**
 * Spark UDF to convert a Quantity from its current unitCode to a target unitCode using UCUM conversions.
 * <p>
 * This UDF wraps {@link FhirPathQuantity#convertToUnit(String)} for use in Spark SQL. It decodes
 * the quantity Row, delegates to the conversion logic, and encodes the result back to a Row.
 * <p>
 * Returns null if either input is null or if the conversion fails.
 *
 * @see FhirPathQuantity#convertToUnit(String)
 * @see QuantityEncoding
 */
public class ConvertQuantityToUnit implements SqlFunction2<Row, String, Row> {

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "convert_quantity_to_unit";

  @Serial
  private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return QuantityEncoding.dataType();
  }

  @Override
  @Nullable
  public Row call(@Nullable final Row quantityRow, @Nullable final String targetUnit) {
    if (quantityRow == null || targetUnit == null) {
      return null;
    }

    // Decode the quantity from Row to FhirPathQuantity
    final FhirPathQuantity quantity = QuantityEncoding.decode(quantityRow);

    // Delegate to FhirPathQuantity.convertToUnit() for conversion logic
    final Optional<FhirPathQuantity> convertedQuantity = quantity.convertToUnit(
        requireNonNull(targetUnit));

    // Encode back to Row (returns null if conversion failed)
    return convertedQuantity.map(QuantityEncoding::encode).orElse(null);
  }
}
