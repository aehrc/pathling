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

package au.csiro.pathling.projection;

import au.csiro.pathling.fhirpath.FhirPath;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.types.DataType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Information about a column that has been requested to be included in a projection.
 * <p>
 * Includes the parsed {@link FhirPath}, the name of the column and whether the column has been
 * asserted to be a collection.
 *
 * @param path The parsed FHIRPath expression that defines the column.
 * @param name The requested name of the column.
 * @param collection Whether the column has been asserted to be a collection.
 * @param type The type that has been asserted for the column.
 * @param sqlType The SQL type that has been asserted for the column.
 */
public record RequestedColumn(
    @Nonnull FhirPath path,
    @Nonnull String name,
    boolean collection,
    @Nonnull Optional<FHIRDefinedType> type,
    @Nonnull Optional<DataType> sqlType
) {

  @Nonnull
  @Override
  public String toString() {
    return "RequestedColumn{" +
        "path=" + path +
        ", name='" + name + '\'' +
        ", collection=" + collection +
        ", type=" + type +
        ", sqlType=" + sqlType +
        '}';
  }

  /**
   * Returns a string expression representation of this requested column.
   *
   * @return the string expression representation
   */
  @Nonnull
  public String toExpression() {
    return (collection
            ? "many"
            : "one") + ": " + path.toExpression() + " as " + name;
  }
}
