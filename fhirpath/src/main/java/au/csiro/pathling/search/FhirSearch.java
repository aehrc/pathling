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
     * Adds a search criterion with the given parameter code and values. The parameter code may
     * include a modifier suffix (e.g., "gender:not" or "family:exact").
     *
     * @param parameterCodeWithModifier the parameter code, optionally with modifier suffix
     * @param values the search values (multiple values = OR logic)
     * @return this builder
     */
    @Nonnull
    public Builder criterion(@Nonnull final String parameterCodeWithModifier,
        @Nonnull final String... values) {
      return criterion(parameterCodeWithModifier, Arrays.asList(values));
    }

    /**
     * Adds a search criterion with the given parameter code and values. The parameter code may
     * include a modifier suffix (e.g., "gender:not" or "family:exact").
     *
     * @param parameterCodeWithModifier the parameter code, optionally with modifier suffix
     * @param values the search values (multiple values = OR logic)
     * @return this builder
     */
    @Nonnull
    public Builder criterion(@Nonnull final String parameterCodeWithModifier,
        @Nonnull final List<String> values) {
      final String[] parts = parseParameterCode(parameterCodeWithModifier);
      criteria.add(SearchCriterion.of(parts[0], parts[1], values));
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
     * Parses a parameter code that may include a modifier suffix.
     *
     * @param parameterCodeWithModifier the parameter code with optional modifier (e.g.,
     *     "gender:not")
     * @return an array of [parameterCode, modifier] where modifier may be null
     */
    @Nonnull
    private String[] parseParameterCode(@Nonnull final String parameterCodeWithModifier) {
      final int colonIndex = parameterCodeWithModifier.indexOf(':');
      if (colonIndex == -1) {
        return new String[]{parameterCodeWithModifier, null};
      }
      return new String[]{
          parameterCodeWithModifier.substring(0, colonIndex),
          parameterCodeWithModifier.substring(colonIndex + 1)
      };
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
