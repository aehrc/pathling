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

import au.csiro.pathling.fhirpath.FhirpathQuantity;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import java.io.Serial;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

/**
 * Spark UDF to parse a FHIRPath quantity literal string to a Quantity struct.
 * <p>
 * Parses strings matching the FHIRPath quantity literal format:
 * <ul>
 *   <li>UCUM units: {@code "value 'unit'"} (e.g., {@code "10 'mg'"}, {@code "1.5 'kg'"})
 *   <li>Calendar duration units: {@code "value unit"} (e.g., {@code "4 days"}, {@code "1 year"})
 * </ul>
 * Returns null if the string cannot be parsed as a valid quantity.
 *
 * @see FhirpathQuantity#parse(String)
 * @see QuantityEncoding#encode(FhirpathQuantity)
 */
public class StringToQuantity implements SqlFunction1<String, Row> {

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "string_to_quantity";

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
  public Row call(@Nullable final String quantityString) {
    if (quantityString == null) {
      return null;
    }
    try {
      final FhirpathQuantity quantity = FhirpathQuantity.parse(quantityString);
      return QuantityEncoding.encode(quantity);
    } catch (final IllegalArgumentException e) {
      // Invalid quantity literal format
      return null;
    }
  }
}
