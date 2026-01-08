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

import static org.apache.spark.sql.functions.lit;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Matches elements using case-sensitive exact equality for the :exact modifier on string search
 * parameters.
 * <p>
 * Per FHIR specification, the :exact modifier on string search requires an exact, case-sensitive
 * match of the entire value.
 *
 * @see <a href="https://hl7.org/fhir/search.html#string">String Search</a>
 */
public class ExactStringMatcher implements ElementMatcher {

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    return element.equalTo(lit(searchValue));
  }
}
