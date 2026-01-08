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

package au.csiro.pathling.search.filter;

import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * Interface for building SparkSQL filter expressions from search parameter values.
 */
public interface SearchFilter {

  /**
   * Builds a SparkSQL Column expression that filters rows based on the search values.
   *
   * @param valueColumn the ColumnRepresentation containing the values to filter on
   * @param searchValues the search values to match (multiple values = OR logic)
   * @return a SparkSQL Column expression that evaluates to true for matching rows
   */
  @Nonnull
  Column buildFilter(@Nonnull ColumnRepresentation valueColumn, @Nonnull List<String> searchValues);
}
