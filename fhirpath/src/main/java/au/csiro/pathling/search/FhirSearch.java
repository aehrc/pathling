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

package au.csiro.pathling.search;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.Value;

/**
 * Represents a FHIR search query consisting of one or more search criteria. Multiple criteria are
 * combined with AND logic.
 * <p>
 * Note: The resource type is not part of the search query itself, but is passed separately to the
 * executor.
 *
 * @see <a href="https://hl7.org/fhir/search.html">FHIR Search</a>
 */
@Value
public class FhirSearch {

  /**
   * The search criteria. Multiple criteria are combined with AND logic.
   */
  @Nonnull
  List<SearchCriterion> criteria;

  /**
   * Creates a new builder for constructing a FhirSearch.
   *
   * @return a new builder
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for constructing FhirSearch instances.
   */
  public static class Builder {

    private final List<SearchCriterion> criteria = new ArrayList<>();

    /**
     * Adds a search criterion with the given parameter code and values.
     *
     * @param parameterCode the parameter code
     * @param values the search values (multiple values = OR logic)
     * @return this builder
     */
    @Nonnull
    public Builder criterion(@Nonnull final String parameterCode,
        @Nonnull final String... values) {
      criteria.add(SearchCriterion.of(parameterCode, Arrays.asList(values)));
      return this;
    }

    /**
     * Adds a search criterion with the given parameter code and values.
     *
     * @param parameterCode the parameter code
     * @param values the search values (multiple values = OR logic)
     * @return this builder
     */
    @Nonnull
    public Builder criterion(@Nonnull final String parameterCode,
        @Nonnull final List<String> values) {
      criteria.add(SearchCriterion.of(parameterCode, values));
      return this;
    }

    /**
     * Adds a pre-built search criterion.
     *
     * @param criterion the criterion to add
     * @return this builder
     */
    @Nonnull
    public Builder criterion(@Nonnull final SearchCriterion criterion) {
      criteria.add(criterion);
      return this;
    }

    /**
     * Builds the FhirSearch instance.
     *
     * @return the constructed FhirSearch
     */
    @Nonnull
    public FhirSearch build() {
      return new FhirSearch(Collections.unmodifiableList(new ArrayList<>(criteria)));
    }
  }
}
