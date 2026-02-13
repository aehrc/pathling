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

package au.csiro.pathling.search.filter;

import static org.apache.spark.sql.functions.lit;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Matches the {@code reference} field of a FHIR Reference element against a search value.
 *
 * <p>Supports three forms of search value:
 *
 * <ul>
 *   <li><b>Absolute URI</b> (contains {@code ://} or starts with {@code urn:}): matched using exact
 *       string equality.
 *   <li><b>Type-qualified</b> (contains {@code /}, e.g. {@code Patient/123}): matched using suffix
 *       semantics with a {@code /} boundary check, or exact equality for relative references.
 *   <li><b>Bare ID</b> (no {@code /}, e.g. {@code 123}): matched by checking that the reference
 *       string ends with {@code /<id>}.
 * </ul>
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/search.html#reference">Reference Search</a>
 */
public class ReferenceMatcher implements ElementMatcher {

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    final Column refCol = element.getField(FhirFieldNames.REFERENCE);

    // Absolute URI: exact string equality.
    if (isAbsoluteUri(searchValue)) {
      return refCol.equalTo(lit(searchValue));
    }

    // Type-qualified (contains /): exact equality for relative references, or endsWith with a
    // "/" boundary to match absolute references without partial type name collisions.
    if (searchValue.contains("/")) {
      return refCol.equalTo(lit(searchValue)).or(refCol.endsWith(lit("/" + searchValue)));
    }

    // Bare ID: reference must end with "/<id>".
    return refCol.endsWith(lit("/" + searchValue));
  }

  /**
   * Checks whether a search value is an absolute URI. An absolute URI contains a scheme component
   * (e.g. {@code http://}, {@code urn:}), which is never present in FHIR type-qualified or bare ID
   * reference values.
   *
   * @param value the search value to check
   * @return true if the value is an absolute URI
   */
  private static boolean isAbsoluteUri(@Nonnull final String value) {
    return value.contains("://") || value.startsWith("urn:");
  }
}
