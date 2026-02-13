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
import java.util.function.BiFunction;
import org.apache.spark.sql.Column;

/**
 * Matches URI elements against a search value using one of three modes: exact equality, prefix
 * matching ({@code :below}), or inverse prefix matching ({@code :above}).
 *
 * <p>Use the static factory methods to create matchers for each mode:
 *
 * <ul>
 *   <li>{@link #exact()} — case-sensitive exact string equality (default for URI search).
 *   <li>{@link #below()} — matches when the element starts with the search value.
 *   <li>{@link #above()} — matches when the search value starts with the element.
 * </ul>
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/search.html#uri">URI Search</a>
 */
public class UriMatcher implements ElementMatcher {

  @Nonnull private final BiFunction<Column, String, Column> matchFunction;

  private UriMatcher(@Nonnull final BiFunction<Column, String, Column> matchFunction) {
    this.matchFunction = matchFunction;
  }

  /**
   * Creates a matcher that uses exact, case-sensitive string equality.
   *
   * @return a URI matcher for exact matching
   */
  @Nonnull
  public static UriMatcher exact() {
    return new UriMatcher((element, searchValue) -> element.equalTo(lit(searchValue)));
  }

  /**
   * Creates a matcher for the {@code :below} modifier. Matches when the element value starts with
   * the search value (URI prefix matching).
   *
   * @return a URI matcher for prefix matching
   */
  @Nonnull
  public static UriMatcher below() {
    return new UriMatcher((element, searchValue) -> element.startsWith(lit(searchValue)));
  }

  /**
   * Creates a matcher for the {@code :above} modifier. Matches when the search value starts with
   * the element value (inverse prefix matching — the element is "above" the search value in the URI
   * hierarchy).
   *
   * @return a URI matcher for inverse prefix matching
   */
  @Nonnull
  public static UriMatcher above() {
    return new UriMatcher((element, searchValue) -> lit(searchValue).startsWith(element));
  }

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    return matchFunction.apply(element, searchValue);
  }
}
