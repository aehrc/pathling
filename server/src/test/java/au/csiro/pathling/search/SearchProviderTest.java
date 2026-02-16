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

package au.csiro.pathling.search;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SearchProvider} utility methods.
 *
 * @author John Grimes
 */
class SearchProviderTest {

  @Test
  void buildQueryStringReturnsNullForNullInput() {
    // Null raw params should produce a null query string.
    assertThat(SearchProvider.buildQueryString(null)).isNull();
  }

  @Test
  void buildQueryStringReturnsNullForEmptyMap() {
    // Empty raw params should produce a null query string.
    assertThat(SearchProvider.buildQueryString(Map.of())).isNull();
  }

  @Test
  void buildQueryStringSingleParameter() {
    // A single parameter should produce a simple key=value string.
    final Map<String, List<String>> params = Map.of("gender", List.of("male"));
    assertThat(SearchProvider.buildQueryString(params)).isEqualTo("gender=male");
  }

  @Test
  void buildQueryStringMultipleParameters() {
    // Multiple parameters should be joined with &.
    final Map<String, List<String>> params = new LinkedHashMap<>();
    params.put("gender", List.of("male"));
    params.put("birthdate", List.of("ge1980-01-01"));

    final String result = SearchProvider.buildQueryString(params);
    assertThat(result).contains("gender=male");
    assertThat(result).contains("birthdate=ge1980-01-01");
    assertThat(result).contains("&");
  }

  @Test
  void buildQueryStringMultiValuedParameter() {
    // A parameter with multiple values (repeated URL parameter) should emit separate pairs.
    final Map<String, List<String>> params = Map.of("status", List.of("active", "inactive"));

    final String result = SearchProvider.buildQueryString(params);
    assertThat(result).contains("status=active");
    assertThat(result).contains("status=inactive");
    assertThat(result).contains("&");
  }

  @Test
  void buildQueryStringPreservesModifierSuffix() {
    // Modifier suffixes in map keys (e.g., gender:not) should be preserved.
    final Map<String, List<String>> params = Map.of("gender:not", List.of("male"));
    assertThat(SearchProvider.buildQueryString(params)).isEqualTo("gender:not=male");
  }
}
